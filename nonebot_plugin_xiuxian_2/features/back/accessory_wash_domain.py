from __future__ import annotations

from copy import deepcopy
from dataclasses import dataclass
from typing import Any


AFFIX_TYPES = ("气血", "抗暴", "防御", "会心", "会心伤害", "攻击", "速度")
WASH_RANGE = {
    1: {"气血": (0.02, 0.05), "抗暴": (0.01, 0.03), "防御": (0.01, 0.03), "会心": (0.01, 0.03), "会心伤害": (0.02, 0.05), "攻击": (0.02, 0.05), "速度": (4, 9)},
    2: {"气血": (0.04, 0.08), "抗暴": (0.02, 0.05), "防御": (0.02, 0.05), "会心": (0.02, 0.05), "会心伤害": (0.04, 0.08), "攻击": (0.04, 0.08), "速度": (8, 16)},
    3: {"气血": (0.06, 0.12), "抗暴": (0.03, 0.07), "防御": (0.03, 0.07), "会心": (0.03, 0.07), "会心伤害": (0.06, 0.12), "攻击": (0.06, 0.12), "速度": (14, 26)},
    4: {"气血": (0.08, 0.16), "抗暴": (0.04, 0.10), "防御": (0.04, 0.10), "会心": (0.04, 0.10), "会心伤害": (0.08, 0.16), "攻击": (0.08, 0.16), "速度": (22, 40)},
    5: {"气血": (0.10, 0.20), "抗暴": (0.05, 0.12), "防御": (0.05, 0.12), "会心": (0.05, 0.12), "会心伤害": (0.10, 0.20), "攻击": (0.10, 0.20), "速度": (34, 60)},
}


@dataclass(frozen=True)
class AccessoryWashPlan:
    status: str
    accessory: dict[str, Any] | None = None


@dataclass(frozen=True)
class AccessoryWashChange:
    status: str
    action: str
    user_id: str
    affected: int = 0
    stone_delta: int = 0
    accessory: dict[str, Any] | None = None

    @property
    def succeeded(self) -> bool:
        return self.status in {"applied", "duplicate"}


def target_affix_count(quality: int) -> int:
    return 3 if max(1, min(5, int(quality))) >= 4 else 2


def wash_stone_cost(quality: int, locked_count: int = 0) -> int:
    base = {1: 1, 2: 2, 3: 4, 4: 8, 5: 12}[max(1, min(5, int(quality)))]
    return base * (1 + max(0, int(locked_count)))


def _roll_affixes(quality: int, count: int, pity_reached: bool, excluded: set[str], random_source: Any) -> list[dict[str, Any]]:
    quality = max(1, min(5, int(quality)))
    candidates = [value for value in AFFIX_TYPES if value not in excluded]
    if len(candidates) < count:
        candidates = list(AFFIX_TYPES)
    selected = random_source.sample(candidates, count)
    result = []
    for affix_type in selected:
        low, high = WASH_RANGE[quality][affix_type]
        value = high if pity_reached else random_source.uniform(low, high)
        result.append({"type": affix_type, "value": round(value) if affix_type == "速度" else round(value, 4)})
    return result


def plan_wash(accessory: dict[str, Any], random_source: Any) -> AccessoryWashPlan:
    quality = max(1, min(5, int(accessory.get("quality", 1))))
    old_affixes = accessory.get("affixes") if isinstance(accessory.get("affixes"), list) else []
    old_affixes = list(old_affixes)[:target_affix_count(quality)]
    locked_raw = accessory.get("locked_affixes", [])
    locked = sorted({int(index) for index in locked_raw if str(index).lstrip("-").isdigit() and 0 <= int(index) < len(old_affixes)}) if isinstance(locked_raw, list) else []
    target_count = target_affix_count(quality)
    if len(locked) >= target_count:
        return AccessoryWashPlan("too_many_locks")
    wash_count = int(accessory.get("wash_count", 0)) + 1
    locked_types = {str(old_affixes[index].get("type", "")) for index in locked if isinstance(old_affixes[index], dict)}
    rolled = iter(_roll_affixes(quality, target_count - len(locked), wash_count >= 150, locked_types, random_source))
    updated_affixes = []
    for index in range(target_count):
        if index in locked and index < len(old_affixes):
            updated_affixes.append(old_affixes[index])
        else:
            updated_affixes.append(next(rolled))
    updated = deepcopy(accessory)
    updated["wash_count"] = wash_count
    updated["affixes"] = updated_affixes
    if locked:
        updated["locked_affixes"] = locked
    else:
        updated.pop("locked_affixes", None)
    return AccessoryWashPlan("applied", updated)


__all__ = ["AccessoryWashChange", "AccessoryWashPlan", "plan_wash", "target_affix_count", "wash_stone_cost"]
