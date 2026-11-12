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


def sqlite_int_compare(left: Any, right: Any) -> int | None:
    """Compare canonical integers without SQLite affinity or REAL rounding.

    Bare NULL remains distinct for nullable snapshots; callers use COALESCE
    when the business field defaults to zero. Only the small comparison result
    crosses SQLite's int64 return boundary. Invalid/non-finite operands return
    SQL NULL so they cannot satisfy a numeric snapshot or balance guard.
    """
    try:
        for operand in (left, right):
            if operand is not None and not Decimal(operand).is_finite():
                return None
    except (InvalidOperation, TypeError, ValueError, OverflowError):
        return None
    if left is None or right is None:
        return (left is not None) - (right is not None)
    left, right = as_int_like(left), as_int_like(right)
    return (left > right) - (left < right)


def register_numeric_functions(connection: Any) -> None:
    connection.create_function("xiuxian_int_cmp", 2, sqlite_int_compare, deterministic=True)
    connection.create_function("xiuxian_int_text", 1, sqlite_int_text, deterministic=True)
    connection.create_function("xiuxian_int_add", 2, _sqlite_int_add, deterministic=True)
    connection.create_function("xiuxian_int_sub", 2, _sqlite_int_sub, deterministic=True)
    connection.create_function("xiuxian_crystal_exact_text", 2, _crystal_exact_text, deterministic=True)
    connection.create_function("xiuxian_crystal_precision_state", 2, _crystal_precision_state, deterministic=True)
    connection.create_collation("xiuxian_integer", lambda left, right: (int(left) > int(right)) - (int(left) < int(right)))
    connection.create_aggregate("xiuxian_int_sum", 1, _IntegerSum)
    connection.create_function("xiuxian_int_abs", 1, lambda value: str(abs(as_int_like(value))), deterministic=True)


class _IntegerSum:
    """Accumulate integer business values without SQLite overflow or REAL rounding."""

    def __init__(self):
        self.total = 0

    def step(self, value):
        self.total += as_int_like(value)

    def finalize(self):
        return str(self.total)


def sqlite_int_text(value: Any) -> str | None:
    """Return a canonical sort value without crossing SQLite's int64 boundary.

    Sort using COLLATE xiuxian_integer; CAST(value AS TEXT) first would lose
    the existing int(REAL) semantics. NULL retains SQLite's usual sort order.
    """
    return None if value is None else str(as_int_like(value))


def _legacy_crystal_value(value: Any, storage_type: Any) -> int | None:
    """Interpret stored legacy values without pretending a rounded REAL is exact."""
    if value is None:
        return 0
    kind = str(storage_type or "").lower()
    if kind == "real":
        try:
            number = float(value)
            # At the boundary itself, an adjacent integer may already have
            # rounded to the same IEEE-754 value (for example 2**53 + 1).
            if not number.is_integer() or abs(number) >= 2**53:
                return None
            return int(number)
        except (TypeError, ValueError, OverflowError):
            return None
    try:
        number = Decimal(str(value).strip())
        if not number.is_finite() or number != number.to_integral_value():
            return None
        return int(number)
    except (InvalidOperation, TypeError, ValueError, OverflowError):
        return None


def exact_crystal_integer(value: Any, storage_type: Any = None) -> int | None:
    """Parse an exact legacy crystal value, rejecting ambiguous SQLite REALs."""
    if storage_type is None:
        storage_type = (
            "null" if value is None else "integer" if isinstance(value, int)
            else "real" if isinstance(value, float) else "text"
        )
    return _legacy_crystal_value(value, storage_type)


def _strict_sql_integer(value: Any) -> int:
    number = exact_crystal_integer(value)
    if number is None:
        raise ValueError("SQLite integer arithmetic requires an exact integer operand")
    return number


def _sqlite_int_add(left: Any, right: Any) -> str:
    return str(_strict_sql_integer(left) + _strict_sql_integer(right))


def _sqlite_int_sub(left: Any, right: Any) -> str:
    return str(_strict_sql_integer(left) - _strict_sql_integer(right))


def _crystal_exact_text(value: Any, storage_type: Any) -> str | None:
    number = _legacy_crystal_value(value, storage_type)
    return None if number is None else str(number)


def _crystal_precision_state(value: Any, storage_type: Any) -> str:
    kind = str(storage_type or "").lower()
    if kind == "real":
        try:
            number = float(value)
            if not number.is_integer():
                return "invalid_numeric_value"
            if abs(number) >= 2**53:
                return "precision_lost_real"
        except (TypeError, ValueError, OverflowError):
            return "invalid_numeric_value"
    elif _legacy_crystal_value(value, storage_type) is None:
        return "invalid_numeric_value"
    return "exact"


def crystal_precision_reason(state: Any) -> str:
    """Return a stable, actionable reason for an unusable legacy balance."""
    if str(state or "") == "precision_lost_real":
        return "crystal_precision_lost_real"
    return "crystal_invalid_numeric_value"


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
    "register_numeric_functions",
    "sqlite_int_compare",
    "sqlite_int_text",
    "_crystal_exact_text",
    "_crystal_precision_state",
    "exact_crystal_integer",
    "crystal_precision_reason",
]
