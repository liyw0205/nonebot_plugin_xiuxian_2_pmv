from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Mapping


@dataclass(frozen=True)
class WorkClaimRequest:
    operation_id: str
    user_id: str
    expected_count: int
    expected_offer: Mapping[str, Any]
    task_index: int
    started_at: str

    def validate(self) -> None:
        if not self.operation_id or not self.user_id or not self.started_at:
            raise ValueError("operation_id, user_id and started_at are required")
        if self.expected_count < 0:
            raise ValueError("expected_count must not be negative")
        if self.task_index <= 0:
            raise ValueError("task_index must be positive")
        if not isinstance(self.expected_offer, Mapping):
            raise ValueError("expected_offer must be an object")

    def payload(self) -> dict[str, Any]:
        return {
            "user_id": self.user_id,
            "expected_count": self.expected_count,
            "task_index": self.task_index,
            "started_at": self.started_at,
        }


@dataclass(frozen=True)
class WorkSettlementRequest:
    operation_id: str
    user_id: str
    expected_work: Mapping[str, Any]
    exp_gain: int
    item: Mapping[str, Any] | None
    max_exp: int
    max_goods_num: int
    success_kind: str = ""
    item_msg: str = ""

    def validate(self) -> None:
        if not self.operation_id or not self.user_id:
            raise ValueError("operation_id and user_id are required")
        if not isinstance(self.expected_work, Mapping) or not self.expected_work.get("scheduled_time"):
            raise ValueError("expected_work with scheduled_time is required")
        if self.exp_gain < 0 or self.max_exp < 0 or self.max_goods_num <= 0:
            raise ValueError("settlement limits are invalid")
        if self.item is not None:
            if not self.item.get("id") or not self.item.get("name") or not self.item.get("type"):
                raise ValueError("item id, name and type are required")

    def payload(self) -> dict[str, Any]:
        return {
            "user_id": self.user_id,
            "expected_work": dict(self.expected_work),
            "exp_gain": self.exp_gain,
            "item": dict(self.item) if self.item is not None else None,
            "max_exp": self.max_exp,
            "max_goods_num": self.max_goods_num,
            "success_kind": self.success_kind,
            "item_msg": self.item_msg,
        }


__all__ = ["WorkClaimRequest", "WorkSettlementRequest"]
