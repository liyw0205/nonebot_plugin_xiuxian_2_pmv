from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Mapping


@dataclass(frozen=True)
class BossPurchaseRequest:
    operation_id: str
    user_id: str
    item_id: int
    item_name: str
    item_type: str
    quantity: int
    unit_cost: int
    weekly_limit: int
    expected_integral: int
    expected_weekly_purchases: Mapping[str, Any]
    max_goods_num: int
    today: Any = None

    def validate(self) -> None:
        if not self.operation_id or not self.user_id or not self.item_name or not self.item_type:
            raise ValueError("operation_id, user_id and item metadata are required")
        if self.item_id <= 0 or self.quantity <= 0:
            raise ValueError("item and quantity must be positive")
        if min(self.unit_cost, self.weekly_limit, self.expected_integral, self.max_goods_num) < 0:
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
            "max_goods_num": self.max_goods_num,
        }


@dataclass(frozen=True)
class BossSettlementRequest:
    operation_id: str
    user_id: str
    expected_bosses: tuple[Mapping[str, Any], ...]
    settled_bosses: tuple[Mapping[str, Any], ...]
    boss_index: int
    expected_stamina: int
    stamina_cost: int
    expected_hp: int
    expected_mp: int
    final_hp: int
    final_mp: int
    expected_exp: int
    exp_reward: int
    expected_stone: int
    stone_reward: int
    expected_daily_stone: int
    expected_daily_integral: int
    expected_total_integral: int
    integral_reward: int
    expected_battle_count: int
    battle_limit: int
    expected_checked_at: str
    checked_at: str
    item: Mapping[str, Any] | None
    max_goods_num: int
    actual_damage: int
    killed: bool
    daily_period: str
    weekly_period: str
    activity_bosses: tuple[Mapping[str, Any], ...] = ()

    def validate(self) -> None:
        if not self.operation_id or not self.user_id:
            raise ValueError("operation_id and user_id are required")
        if not self.expected_bosses or not 0 <= self.boss_index < len(self.expected_bosses):
            raise ValueError("boss index is invalid")
        if min(
            self.expected_stamina, self.stamina_cost, self.expected_hp, self.expected_mp,
            self.final_hp, self.final_mp, self.expected_exp, self.exp_reward,
            self.expected_stone, self.stone_reward, self.expected_daily_stone,
            self.expected_daily_integral, self.expected_total_integral, self.integral_reward,
            self.expected_battle_count, self.battle_limit, self.max_goods_num, self.actual_damage,
        ) < 0:
            raise ValueError("settlement quantities must not be negative")
        if not self.expected_checked_at or not self.checked_at or not self.daily_period or not self.weekly_period:
            raise ValueError("settlement periods and timestamps are required")

    def payload(self) -> dict[str, Any]:
        return {
            "user_id": self.user_id,
            "boss_index": self.boss_index,
            "stamina_cost": self.stamina_cost,
            "battle_limit": self.battle_limit,
            "expected_checked_at": self.expected_checked_at,
            "checked_at": self.checked_at,
            "max_goods_num": self.max_goods_num,
            "daily_period": self.daily_period,
            "weekly_period": self.weekly_period,
        }


__all__ = ["BossPurchaseRequest", "BossSettlementRequest"]
