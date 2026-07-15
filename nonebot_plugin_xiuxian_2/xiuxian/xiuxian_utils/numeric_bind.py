"""SQLite-safe numeric binding for economy / combat / score fields.

History: ``number_count`` lived only on ``xiuxian2_handle`` write helpers.
After atomic ``transaction_service`` paths, many plugins write large counters
(exp, stone, contribution, scores, combat power, …) directly and skipped that
normalization, causing:

    OverflowError: Python int too large to convert to SQLite INTEGER

Rules:
1. Bind-time safety belongs in ``db_backend._adapt_param`` (all SQL params) —
   every oversized Python int is rewritten, regardless of column name.
2. Domain helpers should still call ``number_count`` / ``sql_num`` for clarity
   and for result payloads that re-bind values.
3. Read-path ``normalize_user_row`` / ``normalize_numeric_row`` coerce scientific
   TEXT back to int for fields that commonly overflow (not only exp).
4. Core table schema (``user_xiuxian`` / ``user_cd`` / …) is still ensured only
   by ``XiuxianDateManage._check_data`` at plugin load — services must not
   recreate core columns; op tables may ``CREATE TABLE IF NOT EXISTS``.
"""

from __future__ import annotations

from typing import Any, Iterable

SQLITE_MAX_INT = 2**63 - 1  # 9_223_372_036_854_775_807
SQLITE_MIN_INT = -(2**63)  # -9_223_372_036_854_775_808

# Columns that historically exceed SQLite INTEGER after high-realm play.
# Write path is covered globally by bind_sqlite_param; this set is for READ
# normalization so entry code can keep using int(row["stone"]) safely.
OVERFLOW_NUM_FIELDS = frozenset(
    {
        # combat / cultivation
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
        # spirit stones / bank / wallet
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
        # contribution / points / scores / honor
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

# backward-compatible alias
_COMBAT_NUM_FIELDS = OVERFLOW_NUM_FIELDS


def number_count(num: Any):
    """
    数据库安全处理：
    如果数值超过 SQLite INTEGER 限制，返回科学计数法字符串。
    否则返回 int。

    适用于修为 / 灵石 / 贡献 / 积分 / 战力等任意大整数计数器。
    """
    try:
        val = float(num)
    except (TypeError, ValueError) as exc:
        raise ValueError("输入必须是数字") from exc

    if val > SQLITE_MAX_INT or val < SQLITE_MIN_INT:
        return "{:.2e}".format(val)
    return int(val)


def sql_num(value: Any):
    """Alias for write-path large counters (exp/stone/score/power/…)."""
    return number_count(value)


def sql_num_nonneg(value: Any):
    out = number_count(value)
    if float(out) < 0:
        raise ValueError("numeric field must be non-negative")
    return out


def bind_sqlite_param(value: Any) -> Any:
    """
    Param adapter for SQLite drivers.

    Only rewrites out-of-range *ints* to scientific TEXT.
    Leaves floats/rates/strings untouched (unlike ``number_count`` which
    coerces every number through ``int`` when in range).

    Column-agnostic: stone, contribution, points, power, exp all covered.
    """
    if isinstance(value, bool):
        return int(value)
    if isinstance(value, int) and (value > SQLITE_MAX_INT or value < SQLITE_MIN_INT):
        return "{:.2e}".format(float(value))
    return value


def as_int_like(value: Any, default: int = 0) -> int:
    """Parse int-like values including scientific TEXT (may lose huge precision)."""
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
    """Coerce overflow-normalized fields on any dict row (user / sect / wallet)."""
    if not isinstance(row, dict):
        return row
    keys = OVERFLOW_NUM_FIELDS if fields is None else frozenset(fields)
    out = dict(row)
    for key in keys:
        if key not in out:
            continue
        out[key] = _coerce_field(out[key])
    return out


def normalize_user_row(row: Any) -> Any:
    """Coerce overflow-normalized user fields so callers can safely int() them.

    Covers exp / power / hp / mp / atk / stone / sect_contribution / scores, …
    Scientific TEXT from ``number_count`` / ``bind_sqlite_param`` becomes int via
    float parse (precision may drop for huge values — same as display path).
    """
    return normalize_numeric_row(row)


def normalize_sect_row(row: Any) -> Any:
    """Sect treasury / scale / combat power may also overflow."""
    return normalize_numeric_row(
        row,
        fields=(
            "sect_materials",
            "sect_used_stone",
            "sect_scale",
            "combat_power",
            "stone",
        ),
    )
