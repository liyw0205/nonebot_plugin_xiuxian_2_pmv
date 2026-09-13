from __future__ import annotations

from pathlib import Path
from typing import Any, Mapping, Sequence

from ...core.errors import ConflictError, DomainError, ValidationError
from ...core.result import OperationOutcome, ReplyPlan
from ...infrastructure.database import DatabaseUnitOfWork, OperationLedger
from ...infrastructure.observability import trace_context
from .domain import CombatSettlementRequest
from .repository import CombatSettlementRepository, LegacyCombatSettlementRepository
from .schemas import CombatSettlementResult


class CombatSettlementApplication:
    action = "combat.settle"

    def __init__(self, game_database: str | Path, player_database: str | Path, *,
                 repository: CombatSettlementRepository | None = None,
                 ledger: OperationLedger | None = None) -> None:
        self.game_database = str(game_database)
        self.player_database = str(player_database)
        self.repository = repository
        self.ledger = ledger or OperationLedger()

    def settle(
        self,
        *,
        operation_id: str,
        user_id: str,
        expected_daily: Mapping[str, Any],
        snapshot: str,
        daily_limit: int,
        stone: int,
        items: Sequence[Mapping[str, Any]],
        max_goods_num: int,
    ) -> OperationOutcome[dict[str, Any]]:
        try:
            request = CombatSettlementRequest.build(
                operation_id=operation_id,
                user_id=user_id,
                expected_daily=expected_daily,
                snapshot=snapshot,
                daily_limit=daily_limit,
                stone=stone,
                items=items,
                max_goods_num=max_goods_num,
            )
        except (TypeError, ValueError) as exc:
            raise ValidationError(str(exc)) from exc
        payload = request.payload()
        with trace_context(operation_id=request.operation_id, user_scope=request.user_id):
            try:
                with DatabaseUnitOfWork(self.game_database, immediate=True) as uow:
                    existing = self.ledger.begin(uow, request.operation_id, self.action, payload)
                    if existing is not None:
                        previous = existing.outcome()
                        if previous is not None:
                            return previous.replay()
                        raise ConflictError("操作正在处理中")
                repository = self.repository or LegacyCombatSettlementRepository(self.game_database, self.player_database)
                raw = repository.settle(
                    request.operation_id,
                    request.user_id,
                    dict(request.expected_daily),
                    request.snapshot,
                    request.daily_limit,
                    request.stone,
                    tuple(dict(item) for item in request.items),
                    request.max_goods_num,
                )
                status = str(getattr(raw, "status", None) or (raw.get("status") if isinstance(raw, dict) else "failed"))
                raw_stone = getattr(raw, "stone", None)
                if raw_stone is None and isinstance(raw, dict):
                    raw_stone = raw.get("stone", 0)
                raw_rewards = getattr(raw, "rewards", None)
                if raw_rewards is None and isinstance(raw, dict):
                    raw_rewards = raw.get("rewards", ())
                rewards = tuple((int(item[0]), int(item[1])) for item in (raw_rewards or ()))
                data = CombatSettlementResult(status, request.operation_id, request.user_id, int(raw_stone or 0), rewards).to_dict()
                if status in {"applied", "duplicate"}:
                    outcome = OperationOutcome.applied(
                        request.operation_id,
                        self.action,
                        data=data,
                        granted={"stone": int(raw_stone or 0), "items": dict(rewards)},
                        audit_category="combat_settlement",
                    )
                else:
                    messages = {
                        "inventory_full": "背包物品已达上限，节点战斗结算尚未领取。",
                        "limit_reached": "今日节点战斗次数已达上限。",
                        "state_changed": "战斗当前状态已更新，请重新执行节点战斗。",
                        "user_missing": "未找到修仙数据，节点战斗未结算。",
                        "not_ready": "战斗结算服务尚未就绪。",
                    }
                    outcome = OperationOutcome.rejected(
                        request.operation_id,
                        self.action,
                        messages.get(status, "节点战斗未结算。"),
                        code=status,
                        data=data,
                        audit_category="combat_settlement",
                    )
                with DatabaseUnitOfWork(self.game_database, immediate=True) as uow:
                    self.ledger.finish(uow, outcome)
                return outcome
            except DomainError:
                raise
            except Exception as exc:
                self.ledger.record_failure(self.game_database, request.operation_id, self.action, payload, str(exc))
                raise

    def reply(self, **kwargs: Any) -> ReplyPlan:
        outcome = self.settle(**kwargs)
        return ReplyPlan(outcome.message or outcome.data, reference=True)


__all__ = ["CombatSettlementApplication"]
