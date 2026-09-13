from __future__ import annotations

from dataclasses import dataclass


@dataclass(frozen=True)
class StoneAdjustmentRequest:
    operation_id: str
    operator_id: str
    user_id: str
    expected_stone: int
    requested_delta: int
    target_name: str = ""

    def validate(self) -> None:
        if not self.operation_id or not self.operator_id or not self.user_id:
            raise ValueError("operation_id, operator_id and user_id are required")
        if self.expected_stone < 0:
            raise ValueError("expected_stone must not be negative")
        if self.requested_delta == 0:
            raise ValueError("requested_delta must not be zero")

    def payload(self) -> dict[str, object]:
        return {
            "operator_id": self.operator_id,
            "user_id": self.user_id,
            "expected_stone": self.expected_stone,
            "requested_delta": self.requested_delta,
            "target_name": self.target_name,
        }


@dataclass(frozen=True)
class ItemGrantRequest:
    operation_id: str
    operator_id: str
    user_id: str
    item_id: int
    item_name: str
    item_type: str
    quantity: int
    expected_quantity: int
    max_goods_num: int
    target_name: str = ""

    def validate(self) -> None:
        if not self.operation_id or not self.operator_id or not self.user_id:
            raise ValueError("operation_id, operator_id and user_id are required")
        if self.item_id <= 0 or not self.item_name or not self.item_type:
            raise ValueError("a valid item is required")
        if self.quantity <= 0 or self.expected_quantity < 0 or self.max_goods_num <= 0:
            raise ValueError("valid quantity snapshot and inventory limit are required")

    def payload(self) -> dict[str, object]:
        return {
            "operator_id": self.operator_id,
            "user_id": self.user_id,
            "item_id": self.item_id,
            "item_name": self.item_name,
            "item_type": self.item_type,
            "quantity": self.quantity,
            "expected_quantity": self.expected_quantity,
            "max_goods_num": self.max_goods_num,
            "target_name": self.target_name,
        }


__all__ = ["ItemGrantRequest", "StoneAdjustmentRequest"]
