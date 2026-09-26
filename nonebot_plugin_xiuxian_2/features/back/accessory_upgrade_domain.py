from __future__ import annotations

from copy import deepcopy
from dataclasses import dataclass
from typing import Any

from .accessory_wash_domain import AFFIX_TYPES, WASH_RANGE


@dataclass(frozen=True)
class AccessoryUpgradePlan:
    status: str
    accessory: dict[str, Any] | None = None
    affected: int = 0


@dataclass(frozen=True)
class AccessoryUpgradeChange:
    status: str
    action: str
    user_id: str
    affected: int = 0
    stone_delta: int = 0
    accessory: dict[str, Any] | None = None

    @property
    def succeeded(self) -> bool:
        return self.status in {"applied", "duplicate"}


def upgrade_material_count(quality: int) -> int:
    quality = max(1, min(5, int(quality)))
    return 1 if quality <= 2 else quality - 1


def _signature(accessory: dict[str, Any]) -> tuple[int, str, str, int]:
    return (
        int(accessory.get("item_id", 0)),
        str(accessory.get("part", "")),
        str(accessory.get("set_type", "")),
        int(accessory.get("quality", 1)),
    )


def _target_affix_count(quality: int) -> int:
    return 3 if max(1, min(5, int(quality))) >= 4 else 2


def _roll_affixes(
    quality: int,
    count: int,
    excluded: set[str],
    random_source: Any,
) -> list[dict[str, Any]]:
    candidates = [value for value in AFFIX_TYPES if value not in excluded]
    if len(candidates) < count:
        candidates = list(AFFIX_TYPES)
    result = []
    for affix_type in random_source.sample(candidates, count):
        low, high = WASH_RANGE[quality][affix_type]
        value = random_source.uniform(low, high)
        result.append(
            {
                "type": affix_type,
                "value": round(value) if affix_type == "速度" else round(value, 4),
            }
        )
    return result


def _fit_affixes(quality: int, affixes: Any, random_source: Any) -> list[dict[str, Any]]:
    target_count = _target_affix_count(quality)
    current = list(affixes)[:target_count] if isinstance(affixes, list) else []
    if len(current) >= target_count:
        return current
    existing_types = {
        str(item.get("type", ""))
        for item in current
        if isinstance(item, dict)
    }
    current.extend(
        _roll_affixes(
            quality,
            target_count - len(current),
            existing_types,
            random_source,
        )
    )
    return current


def _normalize_locked(accessory: dict[str, Any], affix_count: int) -> list[int]:
    raw = accessory.get("locked_affixes", [])
    if not isinstance(raw, list):
        return []
    result = []
    for value in raw:
        try:
            index = int(value)
        except (TypeError, ValueError):
            continue
        if 0 <= index < affix_count and index not in result:
            result.append(index)
    return sorted(result)


def plan_upgrade(
    expected_equipped: dict[str, Any],
    expected_bag: list[dict[str, Any]],
    part: str,
    material_uids: tuple[str, ...],
    random_source: Any,
) -> AccessoryUpgradePlan:
    main = expected_equipped.get(part)
    if not isinstance(main, dict):
        return AccessoryUpgradePlan("accessory_missing")

    quality = int(main.get("quality", 1))
    if quality >= 5:
        return AccessoryUpgradePlan("max_quality")

    material_uids = tuple(str(uid).strip() for uid in material_uids)
    required = upgrade_material_count(quality)
    if len(material_uids) != required or len(set(material_uids)) != len(material_uids):
        return AccessoryUpgradePlan("material_mismatch")

    by_uid: dict[str, list[dict[str, Any]]] = {}
    for item in expected_bag:
        if isinstance(item, dict):
            by_uid.setdefault(str(item.get("uid", "")), []).append(item)
    main_signature = _signature(main)
    for uid in material_uids:
        matches = by_uid.get(uid, [])
        if len(matches) != 1:
            return AccessoryUpgradePlan("material_missing")
        if _signature(matches[0]) != main_signature:
            return AccessoryUpgradePlan("material_mismatch")

    upgraded = deepcopy(main)
    upgraded["quality"] = quality + 1
    upgraded["wash_count"] = 0
    upgraded["affixes"] = _fit_affixes(upgraded["quality"], upgraded.get("affixes"), random_source)
    locked = _normalize_locked(upgraded, len(upgraded["affixes"]))
    if locked:
        upgraded["locked_affixes"] = locked
    else:
        upgraded.pop("locked_affixes", None)
    return AccessoryUpgradePlan("applied", upgraded, required)


__all__ = [
    "AccessoryUpgradeChange",
    "AccessoryUpgradePlan",
    "plan_upgrade",
    "upgrade_material_count",
]
