from __future__ import annotations

from datetime import datetime
from typing import Any, Mapping


def parse_tianti_time(value: Any) -> datetime | None:
    if not value:
        return None
    if isinstance(value, datetime):
        return value
    for fmt in ("%Y-%m-%d %H:%M:%S", "%Y-%m-%d %H:%M:%S.%f"):
        try:
            return datetime.strptime(str(value), fmt)
        except ValueError:
            continue
    return None


def calc_qiaoxue_bonus(data: Mapping[str, Any]) -> tuple[float, float]:
    base_ratio = 0.0
    gain_pct = 0.0
    for item in data.get("opened_qiaoxue_detail", []) or []:
        if not isinstance(item, Mapping):
            continue
        effect_type = item.get("effect_type")
        effect_value = float(item.get("effect_value", 0))
        if effect_type == "base_per_min_ratio":
            base_ratio += effect_value
        elif effect_type == "hp_gain_pct":
            gain_pct += effect_value
    return base_ratio, gain_pct


def get_active_medicine_bath(data: Mapping[str, Any], now: datetime) -> dict[str, Any] | None:
    end_time = parse_tianti_time(data.get("medicine_end_time"))
    if end_time is None or now > end_time:
        return None
    try:
        effect = float(data.get("medicine_effect", 0) or 0)
    except (TypeError, ValueError, OverflowError):
        effect = 0.0
    if effect <= 1:
        return None
    return {
        "name": data.get("medicine_name") or "未知药材",
        "effect": effect,
        "end_time": end_time,
    }


def get_sect_fairyland_bonus(level: int) -> float:
    try:
        normalized_level = int(level or 0)
    except (TypeError, ValueError, OverflowError):
        normalized_level = 0
    return max(0, min(normalized_level, 10)) * 0.05


def calculate_tianti_gain_rate(
    data: Mapping[str, Any],
    *,
    base_per_min: int,
    now: datetime,
    sect_fairyland_level: int = 0,
    spirit_vein_multiplier: float = 1.0,
) -> dict[str, Any]:
    base_per_min = int(base_per_min)
    base_ratio, gain_pct = calc_qiaoxue_bonus(data)
    real_per_min = int(base_per_min * (1 + base_ratio))
    bath = get_active_medicine_bath(data, now)
    bath_effect = bath["effect"] if bath else 1.0
    sect_bonus = get_sect_fairyland_bonus(sect_fairyland_level)
    spirit_vein_multiplier = float(spirit_vein_multiplier)
    per_min = int(real_per_min * (1 + gain_pct) * bath_effect * (1 + sect_bonus) * spirit_vein_multiplier)
    return {
        "base_per_min": base_per_min,
        "base_ratio": base_ratio,
        "gain_pct": gain_pct,
        "bath": bath,
        "bath_effect": bath_effect,
        "sect_bonus": sect_bonus,
        "spirit_vein_bonus": spirit_vein_multiplier - 1,
        "per_min": per_min,
        "efficiency": (per_min / base_per_min) if base_per_min > 0 else 0,
    }


__all__ = [
    "calc_qiaoxue_bonus",
    "calculate_tianti_gain_rate",
    "get_active_medicine_bath",
    "get_sect_fairyland_bonus",
    "parse_tianti_time",
]
