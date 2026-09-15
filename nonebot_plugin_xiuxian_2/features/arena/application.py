from __future__ import annotations

from dataclasses import asdict, is_dataclass
from pathlib import Path
from typing import Any, Mapping

from ...core.errors import ConflictError, DomainError, ValidationError
from ...core.result import OperationOutcome, ReplyPlan
from ...infrastructure.database import DatabaseUnitOfWork, OperationLedger
from ...infrastructure.clock import SystemClock
from ...infrastructure.observability import trace_context
from .domain import ArenaChallengePurchaseRequest, ArenaPurchaseRequest, ArenaSettlementRequest
from .repository import ArenaChallengePurchaseSqlRepository, ArenaRepository, LegacyArenaRepository


def _data(raw: Any) -> dict[str, Any]:
    if is_dataclass(raw):
        return dict(asdict(raw))
    if isinstance(raw, Mapping):
        return dict(raw)
    return dict(vars(raw))


class ArenaApplication:
    def __init__(self, game_database: str | Path, player_database: str | Path, *, repository: ArenaRepository | None = None, ledger: OperationLedger | None = None, clock=None) -> None:
        self.game_database = str(game_database)
        self.player_database = str(player_database)
        self.repository = repository
        self.ledger = ledger or OperationLedger()
        self.clock = clock or SystemClock()

    def _repository(self) -> ArenaRepository:
        return self.repository or ArenaChallengePurchaseSqlRepository(self.game_database, self.player_database, clock=self.clock)

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
                if status in {"applied", "duplicate"}:
                    outcome = OperationOutcome.applied(operation_id, action, data=data, consumed={"stone": int(data.get("cost", 0) or 0)}, granted={"score": int(data.get("score_delta", 0) or 0)}, audit_category="arena")
                else:
                    outcome = OperationOutcome.rejected(operation_id, action, "竞技场操作未完成。", code=status, data=data, audit_category="arena")
                with DatabaseUnitOfWork(self.game_database, immediate=True) as uow:
                    self.ledger.finish(uow, outcome)
                return outcome
            except DomainError:
                raise
            except Exception as exc:
                self.ledger.record_failure(self.game_database, operation_id, action, payload, str(exc))
                raise

    def purchase(self, *, operation_id: str, user_id: str, item_id: int, item_name: str, item_type: str, quantity: int, unit_cost: int, weekly_limit: int, expected_honor: int, expected_weekly_purchases: Mapping[str, Any], max_goods_num: int, bind_flag: int = 1, today: Any = None) -> OperationOutcome[dict[str, Any]]:
        try:
            request = ArenaPurchaseRequest(str(operation_id).strip(), str(user_id).strip(), int(item_id), str(item_name), str(item_type), int(quantity), int(unit_cost), int(weekly_limit), int(expected_honor), dict(expected_weekly_purchases or {}), int(max_goods_num), int(bind_flag))
            request.validate()
        except (TypeError, ValueError) as exc:
            raise ValidationError(str(exc)) from exc
        return self._execute(operation_id=request.operation_id, user_id=request.user_id, action="arena.purchase", payload=request.payload(), call=lambda: self._repository().purchase(request.operation_id, request.user_id, request.item_id, request.item_name, request.item_type, request.quantity, request.unit_cost, request.weekly_limit, request.expected_honor, request.expected_weekly_purchases, request.max_goods_num, request.bind_flag, today))

    def purchase_challenges(self, *, operation_id: str, user_id: str, amount: int, unit_cost: int, daily_limit: int, expected_stone: int, expected_bought: int, expected_extra: int, expected_last_buy_date: str, today: Any = None) -> OperationOutcome[dict[str, Any]]:
        try:
            request = ArenaChallengePurchaseRequest(str(operation_id).strip(), str(user_id).strip(), int(amount), int(unit_cost), int(daily_limit), int(expected_stone), int(expected_bought), int(expected_extra), str(expected_last_buy_date), today)
            request.validate()
        except (TypeError, ValueError) as exc:
            raise ValidationError(str(exc)) from exc
        return self._execute(operation_id=request.operation_id, user_id=request.user_id, action="arena.challenge_purchase", payload=request.payload(), call=lambda: self._repository().purchase_challenges(request.operation_id, request.user_id, request.amount, request.unit_cost, request.daily_limit, request.expected_stone, request.expected_bought, request.expected_extra, request.expected_last_buy_date, request.today))

    def use_challenge_ticket(self, *, operation_id: str, user_id: str, item_id: int, requested_count: int, expected_item_count: int, expected_challenges_used: int, expected_extra_challenges: int, challenge_cap: int) -> OperationOutcome[dict[str, Any]]:
        payload = {"user_id": str(user_id), "item_id": int(item_id), "requested_count": int(requested_count), "challenge_cap": int(challenge_cap)}
        return self._execute(operation_id=str(operation_id), user_id=str(user_id), action="arena.challenge_ticket", payload=payload, call=lambda: self._repository().use_challenge_ticket(operation_id, user_id, item_id, requested_count, expected_item_count, expected_challenges_used, expected_extra_challenges, challenge_cap))

    def settlement_result(self, *, operation_id: str, challenger_id: str) -> Any:
        return self._repository().settlement_result(operation_id, challenger_id)

    def settle(self, *, operation_id: str, challenger_id: str, opponent_id: str | None, outcome: str, challenge_cap: int, stamina_cost: int, challenged_at: str, expected_challenger_arena: Mapping[str, Any], expected_opponent_arena: Mapping[str, Any] | None, expected_challenger_player: Mapping[str, Any], expected_opponent_player: Mapping[str, Any] | None, final_challenger_hp: int, final_challenger_mp: int, final_opponent_hp: int | None, final_opponent_mp: int | None, win_points: int, lose_points: int, no_match_points: int) -> OperationOutcome[dict[str, Any]]:
        try:
            request = ArenaSettlementRequest(str(operation_id).strip(), str(challenger_id).strip(), None if opponent_id is None else str(opponent_id), str(outcome), int(challenge_cap), int(stamina_cost), str(challenged_at), dict(expected_challenger_arena), None if expected_opponent_arena is None else dict(expected_opponent_arena), dict(expected_challenger_player), None if expected_opponent_player is None else dict(expected_opponent_player), int(final_challenger_hp), int(final_challenger_mp), None if final_opponent_hp is None else int(final_opponent_hp), None if final_opponent_mp is None else int(final_opponent_mp), int(win_points), int(lose_points), int(no_match_points))
            request.validate()
        except (TypeError, ValueError) as exc:
            raise ValidationError(str(exc)) from exc
        return self._execute(operation_id=request.operation_id, user_id=request.challenger_id, action="arena.settle", payload=request.payload(), call=lambda: self._repository().settle(request.operation_id, request.challenger_id, request.opponent_id, request.outcome, request.challenge_cap, request.stamina_cost, request.challenged_at, request.expected_challenger_arena, request.expected_opponent_arena, request.expected_challenger_player, request.expected_opponent_player, request.final_challenger_hp, request.final_challenger_mp, request.final_opponent_hp, request.final_opponent_mp, request.win_points, request.lose_points, request.no_match_points))

    def reply(self, **kwargs: Any) -> ReplyPlan:
        action = str(kwargs.pop("action", "settle"))
        return ReplyPlan(getattr(self, action)(**kwargs).data, reference=True)


__all__ = ["ArenaApplication"]
