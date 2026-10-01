from __future__ import annotations

from dataclasses import asdict, is_dataclass
from pathlib import Path
from typing import Any, Mapping

from ...core.errors import ConflictError, DomainError, ValidationError
from ...core.result import OperationOutcome, ReplyPlan
from ...infrastructure.database import DatabaseUnitOfWork, OperationLedger
from ...infrastructure.observability import trace_context
from .domain import PuppetPurchaseRequest, PuppetUpgradeRequest
from .repository import LegacyPuppetRepository, PuppetRepository
from .purchase_repository import PuppetPurchaseSqlRepository
from .harvest_repository import PuppetHarvestSqlRepository
from .status_repository import PuppetEnabledUser, PuppetStatusResult, PuppetStatusSqlRepository


def _data(raw: Any) -> dict[str, Any]:
    if is_dataclass(raw):
        return dict(asdict(raw))
    if isinstance(raw, Mapping):
        return dict(raw)
    return dict(vars(raw))


class PuppetApplication:
    def __init__(
        self,
        game_database: str | Path,
        player_database: str | Path,
        *,
        repository: PuppetRepository | None = None,
        status_repository: PuppetStatusSqlRepository | None = None,
        ledger: OperationLedger | None = None,
    ) -> None:
        self.game_database = str(game_database)
        self.player_database = str(player_database)
        self.repository = repository
        self.ledger = ledger or OperationLedger()
        self.status_repository = status_repository or PuppetStatusSqlRepository(
            self.game_database, ledger=self.ledger
        )

    def set_enabled(self, *, operation_id: str, user_id: str, enabled: int | bool) -> PuppetStatusResult:
        return self.status_repository.set_enabled(operation_id, user_id, enabled)

    def get_status(self, user_id: str) -> int:
        return self.status_repository.get_status(user_id)

    def enabled_user_high_watermark(self) -> int:
        return self.status_repository.enabled_user_high_watermark()

    def list_enabled_users(
        self,
        after_row_id: int,
        through_row_id: int,
        *,
        limit: int = PuppetStatusSqlRepository.DEFAULT_PAGE_SIZE,
    ) -> list[PuppetEnabledUser]:
        return self.status_repository.list_enabled_users(
            after_row_id,
            through_row_id,
            limit=limit,
        )

    def _execute(self, *, operation_id: str, user_id: str, action: str, payload: Mapping[str, Any], call) -> OperationOutcome[dict[str, Any]]:
        with trace_context(operation_id=operation_id, user_scope=user_id):
            try:
                with DatabaseUnitOfWork(self.game_database, immediate=True) as uow:
                    existing = self.ledger.begin(uow, operation_id, action, payload)
                    if existing is not None:
                        previous = existing.outcome()
                        if previous is not None:
                            return previous.replay()
                        raise ConflictError("操作正在处理中")
                data = _data(call())
                status = str(data.get("status", "failed"))
                normalized = {"status": status, "user_id": user_id, "action": str(data.get("action", "")), "previous_level": int(data.get("previous_level", 0) or 0), "current_level": int(data.get("current_level", 0) or 0), "stone_cost": int(data.get("stone_cost", 0) or 0)}
                if status in {"purchased", "upgraded", "duplicate"}:
                    outcome = OperationOutcome.applied(operation_id, action, data=normalized, consumed={"stone": normalized["stone_cost"]}, audit_category="puppet")
                else:
                    outcome = OperationOutcome.rejected(operation_id, action, "傀儡操作未完成。", code=status, data=normalized, audit_category="puppet")
                with DatabaseUnitOfWork(self.game_database, immediate=True) as uow:
                    self.ledger.finish(uow, outcome)
                return outcome
            except DomainError:
                raise
            except Exception as exc:
                self.ledger.record_failure(self.game_database, operation_id, action, payload, str(exc))
                raise

    def purchase(self, *, operation_id: str, user_id: str, stone_cost: int) -> OperationOutcome[dict[str, Any]]:
        try:
            request = PuppetPurchaseRequest(str(operation_id).strip(), str(user_id).strip(), int(stone_cost))
            request.validate()
        except (TypeError, ValueError) as exc:
            raise ValidationError(str(exc)) from exc
        if self.repository is None:
            call = lambda: PuppetPurchaseSqlRepository(self.game_database, self.player_database).purchase(request.operation_id, request.user_id, request.stone_cost)
        else:
            repository = self.repository
            call = lambda: repository.purchase(request.operation_id, request.user_id, request.stone_cost)
        return self._execute(operation_id=request.operation_id, user_id=request.user_id, action="puppet.purchase", payload=request.payload(), call=call)

    def upgrade(self, *, operation_id: str, user_id: str, upgrade_costs: Mapping[int, int], max_level: int) -> OperationOutcome[dict[str, Any]]:
        try:
            request = PuppetUpgradeRequest(str(operation_id).strip(), str(user_id).strip(), {int(k): int(v) for k, v in upgrade_costs.items()}, int(max_level))
            request.validate()
        except (TypeError, ValueError) as exc:
            raise ValidationError(str(exc)) from exc
        if self.repository is None:
            call = lambda: PuppetPurchaseSqlRepository(self.game_database, self.player_database).upgrade(
                request.operation_id, request.user_id, dict(request.upgrade_costs), max_level=request.max_level
            )
        else:
            repository = self.repository
            call = lambda: repository.upgrade(request.operation_id, request.user_id, dict(request.upgrade_costs), max_level=request.max_level)
        return self._execute(operation_id=request.operation_id, user_id=request.user_id, action="puppet.upgrade", payload=request.payload(), call=call)

    def harvest(self, *, operation_id: str, user_id: str, **kwargs: Any):
        if self.repository is not None:
            return self.repository.harvest(operation_id, user_id, **kwargs)
        repository = PuppetHarvestSqlRepository(self.game_database, self.player_database, max_goods_num=kwargs.pop("max_goods_num"))
        return repository.harvest(user_id, operation_id=operation_id, **kwargs)

    def reply(self, **kwargs: Any) -> ReplyPlan:
        action = str(kwargs.pop("action", "purchase"))
        return ReplyPlan(getattr(self, action)(**kwargs).data, reference=True)


__all__ = ["PuppetApplication"]
