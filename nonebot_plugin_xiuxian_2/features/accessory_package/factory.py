from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
from typing import Any, Callable, Mapping, Sequence


@dataclass(frozen=True)
class AccessoryInstanceFactory:
    """Build accessory DTOs without global time/random/item state."""

    clock: Any
    id_generator: Callable[[datetime], str]
    item_lookup: Callable[[int], Mapping[str, Any] | None]
    affix_roller: Callable[[int], Sequence[Any]]

    def create(self, item_id: int, quality: int = 1) -> dict[str, Any]:
        item = self.item_lookup(int(item_id))
        if item is None:
            raise ValueError(f"accessory item not found: {item_id}")
        quality = max(1, min(5, int(quality)))
        now = self.clock.now() if hasattr(self.clock, "now") else self.clock()
        if not isinstance(now, datetime):
            raise TypeError("clock must return datetime")
        return {
            "uid": self.id_generator(now),
            "item_id": int(item_id),
            "name": str(item.get("name", "")),
            "part": str(item.get("part", "")),
            "set_type": str(item.get("set_type", "")),
            "quality": quality,
            "affixes": list(self.affix_roller(quality)),
            "locked_affixes": [],
            "wash_count": 0,
        }


__all__ = ["AccessoryInstanceFactory"]
