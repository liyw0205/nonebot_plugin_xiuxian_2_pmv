from __future__ import annotations

from copy import deepcopy
from dataclasses import dataclass
from typing import Any


SLOTS = ("手镯", "戒指", "手环", "项链")


@dataclass(frozen=True)
class AccessoryPresetPlan:
    status: str
    preset_idx: int
    expected_equipped: dict[str, Any]
    expected_preset: dict[str, str | None]
    preset: dict[str, str | None]


@dataclass(frozen=True)
class AccessoryPresetChange:
    status: str
    action: str
    user_id: str
    affected: int = 0
    details: dict[str, Any] | None = None

    @property
    def succeeded(self) -> bool:
        return self.status in {"applied", "duplicate"}


def _normalize_equipped(value: Any) -> dict[str, Any]:
    source = value if isinstance(value, dict) else {}
    return {slot: deepcopy(source.get(slot)) for slot in SLOTS}


def normalize_preset(value: Any) -> dict[str, str | None]:
    source = value if isinstance(value, dict) else {}
    result: dict[str, str | None] = {}
    for slot in SLOTS:
        item = source.get(slot)
        result[slot] = None if item is None or item == "" else str(item)
    return result


def plan_save_preset(
    preset_idx: int,
    expected_equipped: dict[str, Any],
    expected_preset: dict[str, Any],
) -> AccessoryPresetPlan:
    try:
        preset_idx = int(preset_idx)
    except (TypeError, ValueError):
        preset_idx = 0
    equipped = _normalize_equipped(expected_equipped)
    current_preset = normalize_preset(expected_preset)
    preset = {
        slot: (
            str(equipped[slot].get("uid"))
            if isinstance(equipped[slot], dict)
            and equipped[slot].get("uid") not in (None, "")
            else None
        )
        for slot in SLOTS
    }
    if preset_idx not in {1, 2, 3}:
        return AccessoryPresetPlan(
            "invalid_preset", preset_idx, equipped, current_preset, preset
        )
    return AccessoryPresetPlan("applied", preset_idx, equipped, current_preset, preset)


__all__ = [
    "AccessoryPresetChange",
    "AccessoryPresetPlan",
    "SLOTS",
    "normalize_preset",
    "plan_save_preset",
]
