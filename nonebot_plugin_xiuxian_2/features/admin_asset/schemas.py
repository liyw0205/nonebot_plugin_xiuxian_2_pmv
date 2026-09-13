from __future__ import annotations

from dataclasses import dataclass
from typing import Any


@dataclass(frozen=True)
class StoneAdjustmentResult:
    status: str
    operation_id: str
    user_id: str
    previous_stone: int = 0
    final_stone: int = 0
    applied_delta: int = 0

    def to_dict(self) -> dict[str, Any]:
        return {
            "status": self.status,
            "operation_id": self.operation_id,
            "user_id": self.user_id,
            "previous_stone": self.previous_stone,
            "final_stone": self.final_stone,
            "applied_delta": self.applied_delta,
        }


@dataclass(frozen=True)
class ItemGrantResult:
    status: str
    operation_id: str
    user_id: str
    item_id: int
    previous_quantity: int = 0
    final_quantity: int = 0
    granted_quantity: int = 0

    def to_dict(self) -> dict[str, Any]:
        return {
            "status": self.status,
            "operation_id": self.operation_id,
            "user_id": self.user_id,
            "item_id": self.item_id,
            "previous_quantity": self.previous_quantity,
            "final_quantity": self.final_quantity,
            "granted_quantity": self.granted_quantity,
        }


__all__ = ["ItemGrantResult", "StoneAdjustmentResult"]
