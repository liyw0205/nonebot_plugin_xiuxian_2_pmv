from __future__ import annotations

from dataclasses import asdict, is_dataclass
from pathlib import Path
from typing import Any, Mapping

from ...core.errors import ConflictError, DomainError, ValidationError
from ...core.result import OperationOutcome, ReplyPlan
from ...infrastructure.database import DatabaseUnitOfWork, OperationLedger
from ...infrastructure.clock import SystemClock
from ...infrastructure.observability import trace_context
from .domain import BossPurchaseRequest, BossSettlementRequest
from .repository import BossPurchaseSqlRepository, BossRepository


def _data(raw: Any) -> dict[str, Any]:
    if is_dataclass(raw):
        return dict(asdict(raw))
    if isinstance(raw, Mapping):
        return dict(raw)
    return dict(vars(raw))


class BossApplication:
    def __init__(self, game_database: str | Path, player_database: str | Path, *, activity_database: str | Path | None = None, repository: BossRepository | None = None, ledger: OperationLedger | None = None, clock=None) -> None:
        self.game_database = str(game_database)
        self.player_database = str(player_database)
        self.activity_database = str(activity_database) if activity_database else None
        self.repository = repository
        self.ledger = ledger or OperationLedger()
        self.clock = clock or SystemClock()

    def _repository(self) -> BossRepository:
        return self.repository or BossPurchaseSqlRepository(self.game_database, self.player_database, self.activity_database, clock=self.clock)

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
                raw = _data(call())
                status = str(raw.get("status", "failed"))
                normalized = {"status": status, **raw}
                if status in {"applied", "duplicate"}:
                    consumed = {"stamina": int(payload.get("stamina_cost", 0) or 0)} if action == "boss.settle" else {"integral": int(raw.get("cost", 0) or 0)}
                    granted = {key: int(raw.get(key, 0) or 0) for key in ("stone", "exp", "integral") if key in raw}
                    outcome = OperationOutcome.applied(operation_id, action, data=normalized, consumed=consumed, granted=granted, audit_category="boss")
                else:
                    outcome = OperationOutcome.rejected(operation_id, action, "世界BOSS操作未完成。", code=status, data=normalized, audit_category="boss")
                with DatabaseUnitOfWork(self.game_database, immediate=True) as uow:
                    self.ledger.finish(uow, outcome)
                return outcome
            except DomainError:
                raise
            except Exception as exc:
                self.ledger.record_failure(self.game_database, operation_id, action, payload, str(exc))
                raise

    def purchase(self, *, operation_id: str, user_id: str, item_id: int, item_name: str, item_type: str, quantity: int, unit_cost: int, weekly_limit: int, expected_integral: int, expected_weekly_purchases: Mapping[str, Any], max_goods_num: int, today: Any = None) -> OperationOutcome[dict[str, Any]]:
        try:
            request = BossPurchaseRequest(str(operation_id).strip(), str(user_id).strip(), int(item_id), str(item_name), str(item_type), int(quantity), int(unit_cost), int(weekly_limit), int(expected_integral), dict(expected_weekly_purchases or {}), int(max_goods_num), today)
            request.validate()
        except (TypeError, ValueError) as exc:
            raise ValidationError(str(exc)) from exc
        return self._execute(operation_id=request.operation_id, user_id=request.user_id, action="boss.purchase", payload=request.payload(), call=lambda: self._repository().purchase(request.operation_id, request.user_id, request.item_id, request.item_name, request.item_type, request.quantity, request.unit_cost, request.weekly_limit, request.expected_integral, request.expected_weekly_purchases, request.max_goods_num, request.today))

    def settle(self, *, operation_id: str | None = None, user_id: str | None = None, **kwargs: Any) -> OperationOutcome[dict[str, Any]]:
        kwargs = {**kwargs, "operation_id": operation_id if operation_id is not None else kwargs.get("operation_id", ""), "user_id": user_id if user_id is not None else kwargs.get("user_id", "")}
        try:
            request = BossSettlementRequest(
                operation_id=str(kwargs.get("operation_id", "")).strip(), user_id=str(kwargs.get("user_id", "")).strip(),
                expected_bosses=tuple(kwargs.get("expected_bosses") or ()), settled_bosses=tuple(kwargs.get("settled_bosses") or ()),
                boss_index=int(kwargs.get("boss_index", 0)), expected_stamina=int(kwargs.get("expected_stamina", 0)), stamina_cost=int(kwargs.get("stamina_cost", 0)),
                expected_hp=int(kwargs.get("expected_hp", 0)), expected_mp=int(kwargs.get("expected_mp", 0)), final_hp=int(kwargs.get("final_hp", 0)), final_mp=int(kwargs.get("final_mp", 0)),
                expected_exp=int(kwargs.get("expected_exp", 0)), exp_reward=int(kwargs.get("exp_reward", 0)), expected_stone=int(kwargs.get("expected_stone", 0)), stone_reward=int(kwargs.get("stone_reward", 0)),
                expected_daily_stone=int(kwargs.get("expected_daily_stone", 0)), expected_daily_integral=int(kwargs.get("expected_daily_integral", 0)), expected_total_integral=int(kwargs.get("expected_total_integral", 0)), integral_reward=int(kwargs.get("integral_reward", 0)),
                expected_battle_count=int(kwargs.get("expected_battle_count", 0)), battle_limit=int(kwargs.get("battle_limit", 0)), expected_checked_at=str(kwargs.get("expected_checked_at", "")), checked_at=str(kwargs.get("checked_at", "")),
                item=kwargs.get("item"), max_goods_num=int(kwargs.get("max_goods_num", 0)), actual_damage=int(kwargs.get("actual_damage", 0)), killed=bool(kwargs.get("killed", False)), daily_period=str(kwargs.get("daily_period", "")), weekly_period=str(kwargs.get("weekly_period", "")), activity_bosses=tuple(kwargs.get("activity_bosses") or ()),
            )
            request.validate()
        except (TypeError, ValueError) as exc:
            raise ValidationError(str(exc)) from exc
        payload = request.payload()
        call = lambda: self._repository().settle(**{**kwargs, "operation_id": request.operation_id, "user_id": request.user_id})
        return self._execute(operation_id=request.operation_id, user_id=request.user_id, action="boss.settle", payload=payload, call=call)

    def settlement_result(self, *, operation_id: str) -> Any:
        return self._repository().settlement_result(operation_id)

    def reply(self, **kwargs: Any) -> ReplyPlan:
        action = str(kwargs.pop("action", "purchase"))
        return ReplyPlan(getattr(self, action)(**kwargs).data, reference=True)


__all__ = ["BossApplication"]
