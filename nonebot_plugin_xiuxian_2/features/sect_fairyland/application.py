from __future__ import annotations

from dataclasses import asdict, is_dataclass
from pathlib import Path
from typing import Any, Mapping

from ...core.errors import ConflictError, DomainError, ValidationError
from ...core.result import OperationOutcome, ReplyPlan
from ...infrastructure.database import DatabaseUnitOfWork, OperationLedger
from ...infrastructure.observability import trace_context
from .domain import FairylandClaimRequest
from .repository import LegacySectFairylandRepository, SectFairylandRepository


def _data(raw: Any) -> dict[str, Any]:
    if is_dataclass(raw):
        return dict(asdict(raw))
    if isinstance(raw, Mapping):
        return dict(raw)
    return dict(vars(raw))


class SectFairylandApplication:
    action = "sect.fairyland_claim"

    def __init__(self, player_database: str | Path, *, repository: SectFairylandRepository | None = None, ledger: OperationLedger | None = None) -> None:
        self.player_database = str(player_database)
        self.repository = repository
        self.ledger = ledger or OperationLedger()

    def claim(
        self,
        *,
        operation_id: str,
        user_id: str,
        sect_id: str,
        day: str,
        level: int,
        minutes: int,
    ) -> OperationOutcome[dict[str, Any]]:
        try:
            request = FairylandClaimRequest(
                str(operation_id).strip(), str(user_id).strip(), str(sect_id).strip(),
                str(day).strip(), int(level), int(minutes),
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
                repository = self.repository or LegacySectFairylandRepository(self.player_database)
                raw = repository.claim(
                    request.operation_id,
                    request.user_id,
                    request.sect_id,
                    request.day,
                    request.level,
                    request.minutes,
                )
                data = _data(raw)
                status = str(data.get("status", "failed"))
                if status in {"claimed", "duplicate"}:
                    outcome = OperationOutcome.applied(
                        request.operation_id,
                        self.action,
                        data=data,
                        after={"tianti_hp": int(data.get("detail", {}).get("new_hp", 0) or 0)},
                        granted={"tianti_hp": int(data.get("detail", {}).get("real_gain", 0) or 0)},
                        audit_category="sect_fairyland",
                    )
                else:
                    messages = {
                        "already_claimed": "今日已经完成过宗门淬体修行。",
                        "state_changed": "宗门淬体状态已更新，请重新领取。",
                        "user_missing": "未找到修仙数据。",
                    }
                    outcome = OperationOutcome.rejected(
                        request.operation_id,
                        self.action,
                        messages.get(status, "宗门淬体修行未完成。"),
                        code=status,
                        data=data,
                        audit_category="sect_fairyland",
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
        outcome = self.claim(**kwargs)
        return ReplyPlan(outcome.message or outcome.data, reference=True)


__all__ = ["SectFairylandApplication"]
