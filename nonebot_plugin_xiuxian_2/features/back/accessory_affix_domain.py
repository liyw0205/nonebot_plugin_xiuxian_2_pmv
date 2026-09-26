from __future__ import annotations

from copy import deepcopy
from dataclasses import dataclass
from typing import Any


@dataclass(frozen=True)
class AccessoryAffixChange:
    status: str
    action: str
    user_id: str
    accessory: dict[str, Any] | None = None

    @property
    def succeeded(self) -> bool:
        return self.status in {"applied", "duplicate"}


@dataclass(frozen=True)
class AccessoryAffixLockPlan:
    status: str
    accessory: dict[str, Any] | None = None


def plan_affix_locks(
    accessory: dict[str, Any], action: str, locked_indexes: tuple[int, ...]
) -> AccessoryAffixLockPlan:
    if action not in {"lock", "unlock"}:
        raise ValueError("action must be lock or unlock")
    affixes = accessory.get("affixes")
    if not isinstance(affixes, list) or not affixes:
        return AccessoryAffixLockPlan("no_affixes")

    indexes = tuple(sorted({int(index) for index in locked_indexes}))
    if any(index < 0 or index >= len(affixes) for index in indexes):
        return AccessoryAffixLockPlan("invalid_index")

    if action == "lock":
        quality = max(1, min(5, int(accessory.get("quality", 1))))
        target_count = 3 if quality >= 4 else 2
        if len(indexes) >= target_count:
            return AccessoryAffixLockPlan("too_many_locks")
    else:
        raw_locked = accessory.get("locked_affixes", [])
        if not isinstance(raw_locked, list) or not raw_locked:
            return AccessoryAffixLockPlan("no_locked_affixes")

    updated = deepcopy(accessory)
    if indexes:
        updated["locked_affixes"] = list(indexes)
    else:
        updated.pop("locked_affixes", None)
    return AccessoryAffixLockPlan("applied", updated)


__all__ = ["AccessoryAffixChange", "AccessoryAffixLockPlan", "plan_affix_locks"]
