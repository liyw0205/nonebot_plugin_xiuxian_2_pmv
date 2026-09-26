from __future__ import annotations

from copy import deepcopy
from dataclasses import dataclass
from typing import Any

from .accessory_preset_domain import SLOTS, normalize_preset


@dataclass(frozen=True)
class AccessoryQuickEquipPlan:
    status: str
    preset_idx: int
    expected_equipped: dict[str, Any]
    expected_bag: list[Any]
    expected_preset: dict[str, str | None]
    equipped: dict[str, Any]
    bag: list[Any]
    preset: dict[str, str | None]
    affected: int = 0
    details: dict[str, Any] | None = None


@dataclass(frozen=True)
class AccessoryQuickEquipChange:
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


def _find(equipped: dict[str, Any], bag: list[Any], uid: str):
    for index, item in enumerate(bag):
        if isinstance(item, dict) and str(item.get("uid", "")) == uid:
            return "bag", index, item
    for slot, item in equipped.items():
        if isinstance(item, dict) and str(item.get("uid", "")) == uid:
            return "equipped", slot, item
    return None, None, None


def plan_quick_equip(
    preset_idx: int,
    expected_equipped: dict[str, Any],
    expected_bag: list[Any],
    expected_preset: dict[str, Any],
) -> AccessoryQuickEquipPlan:
    try:
        preset_idx = int(preset_idx)
    except (TypeError, ValueError):
        preset_idx = 0
    equipped = _normalize_equipped(expected_equipped)
    bag = deepcopy(expected_bag) if isinstance(expected_bag, list) else []
    preset = normalize_preset(expected_preset)
    if preset_idx not in {1, 2, 3}:
        return AccessoryQuickEquipPlan(
            "invalid_preset", preset_idx, equipped, bag, preset, equipped, bag, preset
        )
    if not any(preset.values()):
        return AccessoryQuickEquipPlan(
            "preset_empty", preset_idx, equipped, bag, preset, equipped, bag, preset
        )

    equipped_results = []
    skipped_results = []
    missing_results = []
    for slot in SLOTS:
        uid = preset.get(slot)
        if not uid:
            continue
        current = equipped.get(slot)
        if isinstance(current, dict) and str(current.get("uid", "")) == uid:
            skipped_results.append({"slot": slot, "reason": "already_equipped"})
            continue

        where, key, target = _find(equipped, bag, uid)
        if target is None:
            preset[slot] = None
            missing_results.append({"slot": slot})
            continue
        if str(target.get("part", "")) != slot:
            skipped_results.append(
                {
                    "slot": slot,
                    "reason": "part_mismatch",
                    "name": str(target.get("name", "未知饰品")),
                }
            )
            continue

        old = equipped.get(slot)
        if where == "bag":
            del bag[key]
        else:
            equipped[key] = None
        if old:
            bag.append(old)
        equipped[slot] = target
        equipped_results.append(
            {"slot": slot, "name": str(target.get("name", "未知饰品"))}
        )

    details = {
        "preset_idx": preset_idx,
        "preset": preset,
        "equipped": equipped_results,
        "skipped": skipped_results,
        "missing": missing_results,
    }
    return AccessoryQuickEquipPlan(
        "applied",
        preset_idx,
        _normalize_equipped(expected_equipped),
        deepcopy(expected_bag) if isinstance(expected_bag, list) else [],
        normalize_preset(expected_preset),
        equipped,
        bag,
        preset,
        len(equipped_results),
        details,
    )


__all__ = [
    "AccessoryQuickEquipChange",
    "AccessoryQuickEquipPlan",
    "plan_quick_equip",
]
