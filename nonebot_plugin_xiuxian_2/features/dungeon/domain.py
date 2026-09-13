from __future__ import annotations

from dataclasses import dataclass
from typing import Any


@dataclass(frozen=True)
class DungeonPurchaseRequest:
    operation_id: str
    user_id: str
    item_id: int
    item_name: str
    item_type: str
    quantity: int
    unit_cost: int
    expected_stone: int
    max_goods: int
    bind_flag: int = 1

    def validate(self) -> None:
        if not self.operation_id or not self.user_id or not self.item_name or not self.item_type:
            raise ValueError("operation_id, user_id and item metadata are required")
        if self.item_id <= 0 or self.quantity <= 0 or self.unit_cost <= 0:
            raise ValueError("item, quantity and cost must be positive")
        if self.expected_stone < 0 or self.max_goods < 0 or self.bind_flag not in {0, 1}:
            raise ValueError("purchase values are invalid")

    def payload(self) -> dict[str, Any]:
        return {"user_id": self.user_id, "item_id": self.item_id, "quantity": self.quantity, "unit_cost": self.unit_cost, "expected_stone": self.expected_stone, "max_goods": self.max_goods, "bind_flag": self.bind_flag}


__all__ = ["DungeonPurchaseRequest"]
