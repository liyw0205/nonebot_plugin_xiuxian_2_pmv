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
        # The operation identity is the settlement event and its expected
        # state. Reward fields and capacity limits are execution inputs: the
        # first committed outcome is authoritative for all later retries.
        return {
            "user_id": self.user_id,
            "expected_work": dict(self.expected_work),
        }


@dataclass(frozen=True)
class WorkRefreshRequest:
    operation_id: str
    user_id: str
    expected_count: int
    expected_cd: Mapping[str, Any]
    expected_offer: Mapping[str, Any] | None
    new_offer: Mapping[str, Any]
    force: bool = False

    def validate(self) -> None:
        if not self.operation_id or not self.user_id:
            raise ValueError("operation_id and user_id are required")
        if self.expected_count <= 0:
            raise ValueError("expected_count must be positive")
        if not isinstance(self.expected_cd, Mapping):
            raise ValueError("expected_cd must be an object")
        if self.expected_offer is not None and not isinstance(self.expected_offer, Mapping):
            raise ValueError("expected_offer must be an object or None")
        if not isinstance(self.new_offer, Mapping) or not self.new_offer.get("tasks"):
            raise ValueError("new_offer with tasks is required")


@dataclass(frozen=True)
class WorkAbortCleanupRequest:
    operation_id: str
    user_id: str
    reason: str
    expected_cd: Mapping[str, Any]
    expected_offer: Mapping[str, Any] | None = None
    expected_stone: int | None = None
    penalty: int = 0

    def validate(self) -> None:
        if not self.operation_id or not self.user_id:
            raise ValueError("operation_id and user_id are required")
        if self.reason not in {"active_abort", "offer_abort", "expired", "reset"}:
            raise ValueError("invalid work cleanup reason")
        if not isinstance(self.expected_cd, Mapping):
            raise ValueError("expected_cd must be an object")
        if self.expected_offer is not None and not isinstance(self.expected_offer, Mapping):
            raise ValueError("expected_offer must be an object or None")
        if self.reason == "active_abort" and self.expected_stone is None:
            raise ValueError("active abort requires a stone snapshot")
        if self.penalty < 0:
            raise ValueError("penalty must not be negative")


__all__ = [
    "WorkAbortCleanupRequest",
    "WorkClaimRequest",
    "WorkSettlementRequest",
    "WorkRefreshRequest",
]
