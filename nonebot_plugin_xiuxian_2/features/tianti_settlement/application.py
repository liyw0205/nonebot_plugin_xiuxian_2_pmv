from __future__ import annotations

from datetime import datetime
from pathlib import Path
from typing import Any

from ...core.errors import ConflictError, DomainError, ValidationError
from ...core.result import OperationOutcome, ReplyPlan
from ...infrastructure.database import DatabaseUnitOfWork, OperationLedger
from ...infrastructure.observability import trace_context
from .domain import TiantiSettlementRequest
from .repository import LegacyTiantiSettlementRepository, TiantiSettlementRepository, TiantiSettlementSqlRepository
from .schemas import TiantiSettlementResult


class TiantiSettlementApplication:
    action = "tianti.settle"

    def __init__(self, player_database: str | Path, *, repository: TiantiSettlementRepository | None = None,
                 ledger: OperationLedger | None = None) -> None:
        self.player_database = str(player_database)
        self.repository = repository
        self.ledger = ledger or OperationLedger()

    def settle(
        self,
        *,
        operation_id: str,
        user_id: str,
        settled_at: datetime,
        sect_fairyland_level: int = 0,
    ) -> OperationOutcome[dict[str, Any]]:
        if not isinstance(settled_at, datetime):
            raise ValidationError("settled_at must be a datetime")
        try:
            request = TiantiSettlementRequest(
                str(operation_id).strip(), str(user_id).strip(), settled_at, int(sect_fairyland_level)
            )
            request.validate()
        except (TypeError, ValueError) as exc:
            raise ValidationError(str(exc)) from exc
        payload = request.payload()
        with trace_context(operation_id=request.operation_id, user_scope=request.user_id):
            try:
                with DatabaseUnitOfWork(self.player_database, immediate=True) as uow:
                    existing = self.ledger.begin(uow, request.operation_id, self.action, payload)
                    if existing is not None:
                        previous = existing.outcome()
                        if previous is not None:
                            return previous.replay()
                        raise ConflictError("操作正在处理中")
                repository = self.repository or TiantiSettlementSqlRepository(self.player_database)
                raw = repository.settle(
                    request.operation_id,
                    request.user_id,
                    request.settled_at,
                    sect_fairyland_level=request.sect_fairyland_level,
                )
                status = str(getattr(raw, "status", None) or (raw.get("status") if isinstance(raw, dict) else "failed"))
                detail = getattr(raw, "detail", None)
                if detail is None and isinstance(raw, dict):
                    detail = raw.get("detail", {})
                detail = dict(detail or {})
                data = TiantiSettlementResult(status, request.operation_id, request.user_id, detail).to_dict()
                if status in {"settled", "duplicate"}:
                    outcome = OperationOutcome.applied(
                        request.operation_id,
                        self.action,
                        data=data,
                        granted={
                            "tianti_hp": int(detail.get("real_gain", 0) or 0),
                        },
                        after={"tianti_hp": int(detail.get("new_hp", 0) or 0)},
                        audit_category="tianti_settlement",
                    )
                else:
                    messages = {
                        "state_changed": "炼体结算状态已变化，请重新执行。",
                        "not_ready": "炼体结算服务尚未就绪。",
                    }
                    outcome = OperationOutcome.rejected(
                        request.operation_id,
                        self.action,
                        messages.get(status, "炼体结算未完成。"),
                        code=status,
                        data=data,
                        audit_category="tianti_settlement",
                    )
                with DatabaseUnitOfWork(self.player_database, immediate=True) as uow:
                    self.ledger.finish(uow, outcome)
                return outcome
            except DomainError:
                raise
            except Exception as exc:
                self.ledger.record_failure(self.player_database, request.operation_id, self.action, payload, str(exc))
                raise

    def reply(self, **kwargs: Any) -> ReplyPlan:
        outcome = self.settle(**kwargs)
        return ReplyPlan(outcome.message or outcome.data, reference=True)


__all__ = ["TiantiSettlementApplication"]
