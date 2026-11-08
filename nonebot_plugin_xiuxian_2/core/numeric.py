from __future__ import annotations

from decimal import Decimal, InvalidOperation
from typing import Any, Iterable, Mapping


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


SQLITE_MAX_INT = 2**63 - 1  # 9_223_372_036_854_775_807
SQLITE_MIN_INT = -(2**63)  # -9_223_372_036_854_775_808


def bind_sqlite_param(value: Any) -> Any:
    """SQLite param guard for oversized integers (int64 limit).

    Python's ``sqlite3`` raises ``OverflowError: Python int too large to convert
    to SQLite INTEGER`` for any int outside int64, which aborts the enclosing
    transaction.  The economy is allowed to produce such values: ``境界.json``
    tops out at ``至高.power = 10**29`` and 修为 caps are ``power ×
    closing_exp_upper_limit``, so 虚神界出关 and the hourly sect grant used to
    fail outright for high realms.

    This adapter binds such values as plain decimal digits, which keeps the
    statement (and the whole settlement) alive.  SQLite's NUMERIC/INTEGER
    affinity columns still store them as REAL (double, ~15-17 significant
    digits) — a historical storage limit, documented in
    ``docs/features/base.md`` ("高境界整数存储边界") — while receipts and
    ``result_json`` keep the full computed integer.  Nothing is clamped or
    truncated to int64.

    Values inside int64 stay ``int`` and round-trip as exact SQLite INTEGER.
    Floats, strings and ``None`` are untouched — rates and ratios must not be
    forced through ``int``.  Booleans normalize to 0/1.
    """
    if isinstance(value, bool):
        return int(value)
    if isinstance(value, int) and (value > SQLITE_MAX_INT or value < SQLITE_MIN_INT):
        return str(value)
    return value


def adapt_sqlite_params(params: Any) -> Any:
    """Apply :func:`bind_sqlite_param` to a params mapping/sequence/scalar."""
    if params is None:
        return ()
    if isinstance(params, Mapping):
        return {key: bind_sqlite_param(value) for key, value in params.items()}
    if isinstance(params, (list, tuple)):
        return tuple(bind_sqlite_param(value) for value in params)
    return (bind_sqlite_param(params),)


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
    "SQLITE_MAX_INT",
    "SQLITE_MIN_INT",
    "adapt_sqlite_params",
    "as_int_like",
    "bind_sqlite_param",
    "normalize_numeric_row",
    "normalize_sect_row",
    "normalize_user_row",
]
