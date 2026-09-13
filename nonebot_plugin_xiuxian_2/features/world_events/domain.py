from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Mapping, Sequence


@dataclass(frozen=True)
class DemonClaimRequest:
    operation_id: str
    event_key: str
    event_id: str
    user_id: str
    expected_claimed: Mapping[str, Any]
    stone: int
    exp: int
    items: tuple[Mapping[str, Any], ...]
    max_goods_num: int

    def validate(self) -> None:
        if not self.operation_id or not self.event_key or not self.event_id or not self.user_id:
            raise ValueError("operation_id, event_key, event_id and user_id are required")
        if self.stone < 0 or self.exp < 0 or self.max_goods_num < 0:
            raise ValueError("rewards and max_goods_num must not be negative")
        if not isinstance(self.expected_claimed, Mapping):
            raise ValueError("expected_claimed must be an object")
        for item in self.items:
            if not isinstance(item, Mapping):
                raise ValueError("reward items must be objects")
            try:
                if int(item.get("id", 0)) <= 0 or int(item.get("amount", 0)) <= 0:
                    raise ValueError("reward item id and amount must be positive")
            except (TypeError, ValueError) as exc:
                raise ValueError("reward item id and amount must be positive") from exc

    def payload(self) -> dict[str, Any]:
        return {
            "event_key": self.event_key,
            "event_id": self.event_id,
            "user_id": self.user_id,
            "expected_claimed": dict(self.expected_claimed),
            "stone": self.stone,
            "exp": self.exp,
            "items": [dict(item) for item in self.items],
            "max_goods_num": self.max_goods_num,
        }


def normalize_items(items: Sequence[Mapping[str, Any]]) -> tuple[dict[str, Any], ...]:
    return tuple(
        {
            "id": int(item["id"]),
            "name": str(item.get("name", "")),
            "type": str(item.get("type", item.get("item_type", ""))),
            "amount": int(item["amount"]),
        }
        for item in items
    )


__all__ = ["DemonClaimRequest", "normalize_items"]
