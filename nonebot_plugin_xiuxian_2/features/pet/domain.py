from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Mapping, Sequence


@dataclass(frozen=True)
class PetTravelClaimRequest:
    operation_id: str
    user_id: str
    expected_travel: Mapping[str, Any]
    stone: int
    exp: int
    items: tuple[Mapping[str, Any], ...]
    max_goods_num: int

    def validate(self) -> None:
        if not self.operation_id or not self.user_id or not isinstance(self.expected_travel, Mapping):
            raise ValueError("operation_id, user_id and expected_travel are required")
        if min(self.stone, self.exp, self.max_goods_num) < 0:
            raise ValueError("rewards and max_goods_num must not be negative")

    def payload(self) -> dict[str, Any]:
        return {"user_id": self.user_id, "expected_travel": dict(self.expected_travel), "stone": self.stone, "exp": self.exp, "items": [dict(item) for item in self.items], "max_goods_num": self.max_goods_num}


@dataclass(frozen=True)
class PetFeedRequest:
    operation_id: str
    user_id: str
    uid: str
    item_id: int
    count: int
    expected: tuple[int, ...]
    updated: tuple[int, ...]

    def validate(self) -> None:
        if not self.operation_id or not self.user_id or not self.uid or self.item_id <= 0 or self.count <= 0:
            raise ValueError("operation, pet, item and count are required")
        if len(self.expected) != len(self.updated):
            raise ValueError("pet snapshots must have the same length")

    def payload(self) -> dict[str, Any]:
        return {"user_id": self.user_id, "uid": self.uid, "item_id": self.item_id, "count": self.count}


__all__ = ["PetTravelClaimRequest", "PetFeedRequest"]
