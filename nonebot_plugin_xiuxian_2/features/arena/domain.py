from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Mapping


@dataclass(frozen=True)
class ArenaPurchaseRequest:
    operation_id: str
    user_id: str
    item_id: int
    item_name: str
    item_type: str
    quantity: int
    unit_cost: int
    weekly_limit: int
    expected_honor: int
    expected_weekly_purchases: Mapping[str, Any]
    max_goods_num: int
    bind_flag: int = 1

    def validate(self) -> None:
        if not self.operation_id or not self.user_id or not self.item_name or not self.item_type:
            raise ValueError("operation, user and item are required")
        if self.item_id < 0 or self.quantity <= 0 or min(self.unit_cost, self.weekly_limit, self.expected_honor, self.max_goods_num) < 0:
            raise ValueError("arena purchase values must be non-negative")

    def payload(self) -> dict[str, Any]:
        return {"user_id": self.user_id, "item_id": self.item_id, "item_name": self.item_name, "item_type": self.item_type, "quantity": self.quantity, "unit_cost": self.unit_cost, "weekly_limit": self.weekly_limit, "max_goods_num": self.max_goods_num, "bind_flag": self.bind_flag}


@dataclass(frozen=True)
class ArenaChallengePurchaseRequest:
    operation_id: str
    user_id: str
    amount: int
    unit_cost: int
    daily_limit: int
    expected_stone: int
    expected_bought: int
    expected_extra: int
    expected_last_buy_date: str
    today: Any = None

    def validate(self) -> None:
        if not self.operation_id or not self.user_id or self.amount <= 0:
            raise ValueError("operation, user and amount are required")
        if min(self.unit_cost, self.daily_limit, self.expected_stone, self.expected_bought, self.expected_extra) < 0:
            raise ValueError("arena challenge purchase values must be non-negative")

    def payload(self) -> dict[str, Any]:
        return {"user_id": self.user_id, "amount": self.amount, "unit_cost": self.unit_cost, "daily_limit": self.daily_limit, "expected_stone": self.expected_stone, "expected_bought": self.expected_bought, "expected_extra": self.expected_extra, "expected_last_buy_date": self.expected_last_buy_date}


@dataclass(frozen=True)
class ArenaSettlementRequest:
    operation_id: str
    challenger_id: str
    opponent_id: str | None
    outcome: str
    challenge_cap: int
    stamina_cost: int
    challenged_at: str
    expected_challenger_arena: Mapping[str, Any]
    expected_opponent_arena: Mapping[str, Any] | None
    expected_challenger_player: Mapping[str, Any]
    expected_opponent_player: Mapping[str, Any] | None
    final_challenger_hp: int
    final_challenger_mp: int
    final_opponent_hp: int | None
    final_opponent_mp: int | None
    win_points: int
    lose_points: int
    no_match_points: int

    def validate(self) -> None:
        if not self.operation_id or not self.challenger_id or not self.challenged_at:
            raise ValueError("operation, challenger and timestamp are required")
        if self.outcome not in {"win", "loss", "draw", "no_match"}:
            raise ValueError("invalid arena outcome")
        if min(self.challenge_cap, self.stamina_cost, self.win_points, self.lose_points, self.no_match_points) < 0:
            raise ValueError("arena settlement values must be non-negative")

    def payload(self) -> dict[str, Any]:
        return {"challenger_id": self.challenger_id, "opponent_id": self.opponent_id, "outcome": self.outcome, "challenge_cap": self.challenge_cap, "stamina_cost": self.stamina_cost, "challenged_at": self.challenged_at}


__all__ = ["ArenaPurchaseRequest", "ArenaChallengePurchaseRequest", "ArenaSettlementRequest"]
