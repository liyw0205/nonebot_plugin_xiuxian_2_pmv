from __future__ import annotations

from dataclasses import asdict, is_dataclass
from pathlib import Path
from typing import Any, Mapping

from ...core.errors import ConflictError, DomainError, ValidationError
from ...core.result import OperationOutcome, ReplyPlan
from ...infrastructure.database import DatabaseUnitOfWork, OperationLedger
from ...infrastructure.observability import trace_context
from .domain import TowerPurchaseRequest, TowerSettlementRequest
from .repository import TowerPurchaseSqlRepository, TowerRepository


def _data(raw: Any) -> dict[str, Any]:
    if is_dataclass(raw):
        return dict(asdict(raw))
    if isinstance(raw, Mapping):
        return dict(raw)
    return dict(vars(raw))


class TowerApplication:
    def __init__(self, game_database: str | Path, player_database: str | Path, *, repository: TowerRepository | None = None, ledger: OperationLedger | None = None) -> None:
        self.game_database = str(game_database)
        self.player_database = str(player_database)
        self.repository = repository
        self.ledger = ledger or OperationLedger()

    def _repository(self) -> TowerRepository:
        return self.repository or TowerPurchaseSqlRepository(self.game_database, self.player_database)

    def _execute(self, *, operation_id: str, user_id: str, action: str, payload: Mapping[str, Any], call, normalize, messages: Mapping[str, str]) -> OperationOutcome[dict[str, Any]]:
        with trace_context(operation_id=operation_id, user_scope=user_id):
            try:
                with DatabaseUnitOfWork(self.game_database, immediate=True) as uow:
                    existing = self.ledger.begin(uow, operation_id, action, payload)
                    if existing is not None:
                        previous = existing.outcome()
                        if previous is not None:
                            return previous.replay()
                        raise ConflictError("操作正在处理中")
                data = normalize(_data(call()))
                status = str(data.get("status", "failed"))
                if status in {"applied", "duplicate"}:
                    outcome = OperationOutcome.applied(operation_id, action, data=data, granted=data.get("granted", {}), audit_category="tower")
                else:
                    outcome = OperationOutcome.rejected(operation_id, action, messages.get(status, "通天塔操作未完成。"), code=status, data=data, audit_category="tower")
                with DatabaseUnitOfWork(self.game_database, immediate=True) as uow:
                    self.ledger.finish(uow, outcome)
                return outcome
            except DomainError:
                raise
            except Exception as exc:
                self.ledger.record_failure(self.game_database, operation_id, action, payload, str(exc))
                raise

    def purchase(self, *, operation_id: str, user_id: str, item_id: int, item_name: str, item_type: str, quantity: int, unit_cost: int, weekly_limit: int, expected_score: int, expected_weekly_purchases: Mapping[str, Any], max_goods_num: int, bind_flag: int = 1, today: Any = None) -> OperationOutcome[dict[str, Any]]:
        try:
            request = TowerPurchaseRequest(str(operation_id).strip(), str(user_id).strip(), int(item_id), str(item_name), str(item_type), int(quantity), int(unit_cost), int(weekly_limit), int(expected_score), dict(expected_weekly_purchases or {}), int(max_goods_num), int(bind_flag), today)
            request.validate()
        except (TypeError, ValueError) as exc:
            raise ValidationError(str(exc)) from exc
        return self._execute(
            operation_id=request.operation_id, user_id=request.user_id, action="tower.purchase", payload=request.payload(),
            call=lambda: self._repository().purchase(request.operation_id, request.user_id, request.item_id, request.item_name, request.item_type, request.quantity, request.unit_cost, request.weekly_limit, request.expected_score, request.expected_weekly_purchases, request.max_goods_num, request.bind_flag, request.today),
            normalize=lambda data: {"status": data.get("status", "failed"), "quantity": int(data.get("quantity", 0) or 0), "cost": int(data.get("cost", 0) or 0), "score": int(data.get("score", 0) or 0), "purchased": int(data.get("purchased", 0) or 0), "inventory": int(data.get("inventory", 0) or 0), "granted": {"item": request.item_id, "quantity": int(data.get("quantity", 0) or 0)}},
            messages={"score_insufficient": "积分不足。", "limit_reached": "该物品已达到每周限购。", "inventory_full": "物品数量已达上限。", "state_changed": "通天塔状态已更新，请重新兑换。", "user_missing": "未找到道友数据。"},
        )

    def settle(self, *, operation_id: str, user_id: str, expected_tower: Mapping[str, Any], floor: int, score: int, stone: int, exp: int, items: Any, max_goods_num: int, expected_player: Mapping[str, Any] | None = None, final_hp: int | None = None, final_mp: int | None = None, stamina_cost: int = 0, challenge_succeeded: bool = True) -> OperationOutcome[dict[str, Any]]:
        try:
            request = TowerSettlementRequest(str(operation_id).strip(), str(user_id).strip(), dict(expected_tower or {}), int(floor), int(score), int(stone), int(exp), tuple(dict(item) for item in (items or ())), int(max_goods_num), dict(expected_player) if expected_player is not None else None, final_hp, final_mp, int(stamina_cost), bool(challenge_succeeded))
            request.validate()
        except (TypeError, ValueError) as exc:
            raise ValidationError(str(exc)) from exc
        return self._execute(
            operation_id=request.operation_id, user_id=request.user_id, action="tower.settle", payload=request.payload(),
            call=lambda: self._repository().settle(request.operation_id, request.user_id, request.expected_tower, request.floor, request.score, request.stone, request.exp, request.items, request.max_goods_num, expected_player=request.expected_player, final_hp=request.final_hp, final_mp=request.final_mp, stamina_cost=request.stamina_cost, challenge_succeeded=request.challenge_succeeded),
            normalize=lambda data: {"status": data.get("status", "failed"), "score": int(data.get("score", 0) or 0), "stone": int(data.get("stone", 0) or 0), "exp": int(data.get("exp", 0) or 0), "floor": int(data.get("floor", 0) or 0), "challenge_succeeded": bool(data.get("challenge_succeeded", True)), "rewards": data.get("rewards", ()), "stamina_cost": int(data.get("stamina_cost", 0) or 0), "granted": {"score": int(data.get("score", 0) or 0), "stone": int(data.get("stone", 0) or 0), "exp": int(data.get("exp", 0) or 0)}},
            messages={"stamina_insufficient": "体力不足。", "inventory_full": "通天塔奖励无法放入背包。", "state_changed": "通天塔状态已更新，请重新挑战。", "user_missing": "未找到道友数据。"},
        )

    def settlement_result(self, *, operation_id: str) -> Any:
        return self._repository().settlement_result(operation_id)

    def reply(self, **kwargs: Any) -> ReplyPlan:
        action = str(kwargs.pop("action", "settle"))
        return ReplyPlan(getattr(self, action)(**kwargs).data, reference=True)


__all__ = ["TowerApplication"]
