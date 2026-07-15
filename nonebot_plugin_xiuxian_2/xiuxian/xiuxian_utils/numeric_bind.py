"""SQLite-safe numeric binding for combat/economy fields.

History: ``number_count`` lived only on ``xiuxian2_handle`` write helpers.
After atomic ``transaction_service`` paths, many plugins write ``user_xiuxian``
directly and skipped that normalization, causing:

    OverflowError: Python int too large to convert to SQLite INTEGER

Rules:
1. Bind-time safety belongs in ``db_backend._adapt_param`` (all SQL params).
2. Domain helpers should still call ``number_count`` / ``sql_num`` for clarity
   and for result payloads that re-bind values.
3. Core table schema (``user_xiuxian`` / ``user_cd`` / …) is still ensured only
   by ``XiuxianDateManage._check_data`` at plugin load — services must not
   recreate core columns; op tables may ``CREATE TABLE IF NOT EXISTS``.
"""

from __future__ import annotations

from typing import Any

SQLITE_MAX_INT = 2**63 - 1  # 9_223_372_036_854_775_807
SQLITE_MIN_INT = -(2**63)  # -9_223_372_036_854_775_808


def number_count(num: Any):
    """
    数据库安全处理：
    如果数值超过 SQLite INTEGER 限制，返回科学计数法字符串。
    否则返回 int。
    """
    try:
        val = float(num)
    except (TypeError, ValueError) as exc:
        raise ValueError("输入必须是数字") from exc

    if val > SQLITE_MAX_INT or val < SQLITE_MIN_INT:
        return "{:.2e}".format(val)
    return int(val)


def sql_num(value: Any):
    """Alias for write-path combat/exp values."""
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


_COMBAT_NUM_FIELDS = frozenset(
    {
        "exp",
        "power",
        "hp",
        "mp",
        "atk",
        "stone",
        "sect_materials",
        "sect_used_stone",
        "sect_scale",
        "combat_power",
    }
)


def normalize_user_row(row: Any) -> Any:
    """Coerce overflow-normalized combat fields so callers can safely int()/math them.

    Scientific TEXT from ``number_count`` / ``bind_sqlite_param`` becomes int via
    float parse (precision may drop for huge values — same as display path).
    """
    if not isinstance(row, dict):
        return row
    out = dict(row)
    for key in _COMBAT_NUM_FIELDS:
        if key not in out:
            continue
        value = out[key]
        if value is None or isinstance(value, bool):
            continue
        if isinstance(value, int):
            continue
        if isinstance(value, float):
            try:
                out[key] = int(value)
            except (OverflowError, ValueError):
                out[key] = as_int_like(value)
            continue
        text = str(value).strip()
        if not text:
            continue
        if any(ch in text for ch in (".", "e", "E")):
            out[key] = as_int_like(text)
        else:
            try:
                out[key] = int(text)
            except (TypeError, ValueError):
                out[key] = as_int_like(text)
    return out

