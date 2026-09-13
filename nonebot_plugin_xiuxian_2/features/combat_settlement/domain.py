from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Mapping


@dataclass(frozen=True)
class CombatSettlementRequest:
    operation_id: str
    user_id: str
    expected_daily: Mapping[str, Any]
    snapshot: str
    daily_limit: int
    stone: int
    items: tuple[Mapping[str, Any], ...]
    max_goods_num: int

    @classmethod
    def build(cls, **kwargs: Any) -> "CombatSettlementRequest":
        items = tuple(dict(item) for item in (kwargs.get("items") or ()))
        request = cls(
            operation_id=str(kwargs.get("operation_id", "")).strip(),
            user_id=str(kwargs.get("user_id", "")).strip(),
            expected_daily=dict(kwargs.get("expected_daily") or {}),
            snapshot=str(kwargs.get("snapshot", "")),
            daily_limit=int(kwargs.get("daily_limit", 0)),
            stone=int(kwargs.get("stone", 0)),
            items=items,
            max_goods_num=int(kwargs.get("max_goods_num", 0)),
        )
        request.validate()
        return request

    def validate(self) -> None:
        if not self.operation_id or not self.user_id or not self.snapshot:
            raise ValueError("operation_id, user_id and snapshot are required")
        if not self.expected_daily.get("date"):
            raise ValueError("expected_daily.date is required")
        if min(self.daily_limit, self.stone, self.max_goods_num) < 0:
            raise ValueError("settlement quantities must not be negative")
        for item in self.items:
            if not {"id", "name", "type", "amount"}.issubset(item):
                raise ValueError("combat reward items require id, name, type and amount")
            if int(item["amount"]) < 0:
                raise ValueError("combat reward amount must not be negative")

    def payload(self) -> dict[str, Any]:
        return {
            "user_id": self.user_id,
            "expected_daily": dict(self.expected_daily),
            "snapshot": self.snapshot,
            "daily_limit": self.daily_limit,
            "stone": self.stone,
            "items": [dict(item) for item in self.items],
            "max_goods_num": self.max_goods_num,
        }


__all__ = ["CombatSettlementRequest"]
