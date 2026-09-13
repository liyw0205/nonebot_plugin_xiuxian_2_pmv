from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Mapping, Sequence


@dataclass(frozen=True)
class TowerPurchaseRequest:
    operation_id: str
    user_id: str
    item_id: int
    item_name: str
    item_type: str
    quantity: int
    unit_cost: int
    weekly_limit: int
    expected_score: int
    expected_weekly_purchases: Mapping[str, Any]
    max_goods_num: int
    bind_flag: int = 1
    today: Any = None

    def validate(self) -> None:
        if not self.operation_id or not self.user_id or not self.item_name or not self.item_type:
            raise ValueError("operation_id, user_id and item metadata are required")
        if self.item_id <= 0 or self.quantity <= 0:
            raise ValueError("item and quantity must be positive")
        if min(self.unit_cost, self.weekly_limit, self.expected_score, self.max_goods_num) < 0:
            raise ValueError("purchase limits must not be negative")
        if not isinstance(self.expected_weekly_purchases, Mapping):
            raise ValueError("expected_weekly_purchases must be an object")

    def payload(self) -> dict[str, Any]:
        return {
            "user_id": self.user_id,
            "item_id": self.item_id,
            "quantity": self.quantity,
            "unit_cost": self.unit_cost,
            "weekly_limit": self.weekly_limit,
            "bind_flag": self.bind_flag,
        }


@dataclass(frozen=True)
class TowerSettlementRequest:
    operation_id: str
    user_id: str
    expected_tower: Mapping[str, Any]
    floor: int
    score: int
    stone: int
    exp: int
    items: tuple[Mapping[str, Any], ...]
    max_goods_num: int
    expected_player: Mapping[str, Any] | None = None
    final_hp: int | None = None
    final_mp: int | None = None
    stamina_cost: int = 0
    challenge_succeeded: bool = True

    def validate(self) -> None:
        if not self.operation_id or not self.user_id:
            raise ValueError("operation_id and user_id are required")
        if not all(key in self.expected_tower for key in ("current_floor", "max_floor", "score")):
            raise ValueError("expected tower state is incomplete")
        if self.floor <= 0 or min(self.score, self.stone, self.exp, self.max_goods_num, self.stamina_cost) < 0:
            raise ValueError("settlement quantities are invalid")
        for item in self.items:
            if not {"id", "name", "type", "amount"}.issubset(item):
                raise ValueError("tower reward items require id, name, type and amount")
            if int(item["amount"]) < 0:
                raise ValueError("reward amount must not be negative")

    def payload(self) -> dict[str, Any]:
        return {
            "user_id": self.user_id,
            "expected_tower": dict(self.expected_tower),
            "floor": self.floor,
            "stamina_cost": self.stamina_cost,
            "challenge_succeeded": self.challenge_succeeded,
        }


__all__ = ["TowerPurchaseRequest", "TowerSettlementRequest"]
