from __future__ import annotations

from decimal import Decimal, InvalidOperation
from typing import Any, Iterable


OVERFLOW_NUM_FIELDS = frozenset(
    {
        "exp",
        "power",
        "combat_power",
        "hp",
        "mp",
        "atk",
        "max_hp",
        "max_mp",
        "base_hp",
        "base_mp",
        "base_atk",
        "final_atk",
        "current_hp",
        "current_mp",
        "tianti_hp",
        "hp_left",
        "total_exp",
        "max_exp",
        "final_exp",
        "exp_day",
        "stone",
        "stone_num",
        "wallet_stone",
        "saved_stone",
        "savestone",
        "stored_stone",
        "remaining_stone",
        "deducted_stone",
        "wishing_stones",
        "boss_stone",
        "previous_stone",
        "stone_cost",
        "stone_delta",
        "sect_used_stone",
        "sect_materials",
        "sect_scale",
        "sect_contribution",
        "contribution",
        "score",
        "points",
        "total_points",
        "honor_points",
        "integral",
        "boss_integral",
        "previous_integral",
    }
)


def as_int_like(value: Any, default: int = 0) -> int:
    """Parse int-like values including scientific TEXT."""
    try:
        if value is None:
            return default
        if isinstance(value, bool):
            return int(value)
        if isinstance(value, int):
            return value
        if isinstance(value, float):
            return int(value)
        text = str(value).strip()
        if not text:
            return default
        if any(ch in text for ch in (".", "e", "E")):
            try:
                return int(Decimal(text))
            except (InvalidOperation, ValueError, OverflowError):
                return int(float(text))
        return int(text)
    except (TypeError, ValueError, OverflowError):
        return default


def _coerce_field(value: Any) -> Any:
    if value is None or isinstance(value, bool):
        return value
    if isinstance(value, int):
        return value
    if isinstance(value, float):
        try:
            return int(value)
        except (OverflowError, ValueError):
            return as_int_like(value)
    text = str(value).strip()
    if not text:
        return value
    if any(ch in text for ch in (".", "e", "E")):
        return as_int_like(text)
    try:
        return int(text)
    except (TypeError, ValueError):
        return as_int_like(text)


def normalize_numeric_row(row: Any, fields: Iterable[str] | None = None) -> Any:
    """Coerce overflow-normalized fields on user, sect, and wallet rows."""
    if not isinstance(row, dict):
        return row
    keys = OVERFLOW_NUM_FIELDS if fields is None else frozenset(fields)
    out = dict(row)
    for key in keys:
        if key in out:
            out[key] = _coerce_field(out[key])
    return out


def normalize_user_row(row: Any) -> Any:
    """Normalize numeric fields historically stored as oversized TEXT."""
    return normalize_numeric_row(row)


def normalize_sect_row(row: Any) -> Any:
    """Normalize sect treasury, scale, and combat-power fields."""
    return normalize_numeric_row(
        row,
        fields=("sect_materials", "sect_used_stone", "sect_scale", "combat_power", "stone"),
    )


__all__ = [
    "OVERFLOW_NUM_FIELDS",
    "as_int_like",
    "normalize_numeric_row",
    "normalize_sect_row",
    "normalize_user_row",
]
