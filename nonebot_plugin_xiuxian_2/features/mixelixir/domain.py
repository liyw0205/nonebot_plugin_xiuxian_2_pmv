from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Mapping, Sequence


@dataclass(frozen=True)
class HarvestRequest:
    operation_id: str
    user_id: str
    expected_last_time: str
    harvested_at: str
    rewards: tuple[Mapping[str, Any], ...]
    max_goods_num: int

    def validate(self) -> None:
        if not self.operation_id or not self.user_id or not self.expected_last_time or not self.harvested_at:
            raise ValueError("operation, user and harvest times are required")
        if not self.rewards or self.max_goods_num <= 0:
            raise ValueError("rewards and capacity are required")
        if any(int(item.get("item_id", 0)) <= 0 or int(item.get("quantity", 0)) <= 0 for item in self.rewards):
            raise ValueError("reward item id and quantity must be positive")

    def payload(self) -> dict[str, Any]:
        return {"user_id": self.user_id, "expected_last_time": self.expected_last_time}


@dataclass(frozen=True)
class SettlementRequest:
    operation_id: str
    user_id: str
    materials: Mapping[int, int]
    reward_id: int
    reward_name: str
    reward_quantity: int
    max_goods_num: int

    def validate(self) -> None:
        if not self.operation_id or not self.user_id or not self.reward_name:
            raise ValueError("operation, user and reward name are required")
        if not self.materials or self.reward_id <= 0 or self.reward_quantity <= 0 or self.max_goods_num <= 0:
            raise ValueError("materials, reward and capacity are required")
        if any(int(quantity) <= 0 for quantity in self.materials.values()):
            raise ValueError("material quantities must be positive")

    def payload(self) -> dict[str, Any]:
        return {
            "user_id": self.user_id,
            "materials": {str(key): int(value) for key, value in self.materials.items()},
            "reward_id": self.reward_id,
            "reward_quantity": self.reward_quantity,
        }


def normalize_rewards(rewards: Sequence[Any]) -> tuple[dict[str, Any], ...]:
    normalized = []
    for reward in rewards:
        if isinstance(reward, Mapping):
            normalized.append({"item_id": int(reward["item_id"]), "name": str(reward["name"]), "quantity": int(reward["quantity"])})
        else:
            normalized.append({"item_id": int(reward[0]), "name": str(reward[1]), "quantity": int(reward[2])})
    return tuple(normalized)


__all__ = ["HarvestRequest", "SettlementRequest", "normalize_rewards"]
