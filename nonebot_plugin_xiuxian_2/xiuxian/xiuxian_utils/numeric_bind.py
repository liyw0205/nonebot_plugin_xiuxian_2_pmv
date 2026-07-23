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

import re
from decimal import Decimal, InvalidOperation
import json
from typing import Any, Iterable

SQLITE_MAX_INT = 2**63 - 1  # 9_223_372_036_854_775_807
SQLITE_MIN_INT = -(2**63)  # -9_223_372_036_854_775_808

_NUM_LIKE_RE = re.compile(
    r"^[+-]?(?:\d+(?:\.\d*)?|\.\d+)(?:[eE][+-]?\d+)?$"
)

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
    若数值超过 SQLite INTEGER 限制，返回纯数字 TEXT（避免 float/{:.2e} 丢精度）。
    否则返回 int。

    适用于修为 / 灵石 / 贡献 / 积分 / 战力等任意大整数计数器。
    读路径 as_int_like / format_plain_number 兼容历史科学计数法 TEXT。
    """
    try:
        if isinstance(num, bool):
            return int(num)
        if isinstance(num, int):
            val_int = num
        elif isinstance(num, float):
            val_int = int(num)
        else:
            text = str(num).strip()
            if not text:
                raise ValueError("输入必须是数字")
            if re.fullmatch(r"[+-]?\d+", text):
                val_int = int(text)
            else:
                # scientific / decimal text
                try:
                    dec = Decimal(text)
                except (InvalidOperation, ValueError) as exc:
                    raise ValueError("输入必须是数字") from exc
                if dec == dec.to_integral_value():
                    val_int = int(dec)
                else:
                    # non-integer rates shouldn't go through number_count often
                    val_int = int(dec)
    except (TypeError, ValueError, OverflowError, InvalidOperation) as exc:
        raise ValueError("输入必须是数字") from exc

    if val_int > SQLITE_MAX_INT or val_int < SQLITE_MIN_INT:
        return str(val_int)
    return val_int


def sql_num(value: Any):
    """Alias for write-path large counters (exp/stone/score/power/…)."""
    return number_count(value)


def sql_num_nonneg(value: Any):
    out = number_count(value)
    if as_int_like(out) < 0:
        raise ValueError("numeric field must be non-negative")
    return out


def bind_sqlite_param(value: Any) -> Any:
    """
    Param adapter for SQLite drivers.

    Oversized ints → plain-digit TEXT (not scientific float).
    Leaves floats/rates/strings untouched (unlike ``number_count`` which
    coerces every number through ``int`` when in range).

    Column-agnostic: stone, contribution, points, power, exp all covered.
    """
    if isinstance(value, bool):
        return int(value)
    if isinstance(value, int) and (value > SQLITE_MAX_INT or value < SQLITE_MIN_INT):
        return str(value)
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
            # Prefer Decimal for scientific TEXT so huge realm values stay usable.
            try:
                return int(Decimal(text))
            except (InvalidOperation, ValueError, OverflowError):
                return int(float(text))
        return int(text)
    except (TypeError, ValueError, OverflowError):
        return default


def format_plain_number(value: Any) -> str:
    """Web/UI display: scientific TEXT/float/int → plain decimal digits (no e+).

    Non-numeric strings are returned unchanged. Empty/None → \"\".
    """
    if value is None:
        return ""
    if isinstance(value, bool):
        return str(int(value))
    if isinstance(value, int):
        return str(value)
    text = str(value).strip()
    if not text:
        return ""
    # already plain integer digits
    if re.fullmatch(r"[+-]?\d+", text):
        return text if not text.startswith("+") else text[1:] or "0"
    if not _NUM_LIKE_RE.fullmatch(text):
        return text
    try:
        dec = Decimal(text)
    except (InvalidOperation, ValueError):
        return text
    # integer-like → no fractional part
    if dec == dec.to_integral_value():
        # format(..., 'f') avoids scientific notation for huge magnitudes
        return format(dec.to_integral_value(), "f").split(".", 1)[0]
    s = format(dec, "f")
    if "." in s:
        s = s.rstrip("0").rstrip(".")
    return s or "0"


def parse_web_number(value: Any) -> Any:
    """Parse Web form input: plain digits or scientific notation → int / number_count.

    Empty → None. Non-numeric text is returned as stripped str (for text columns).
    """
    if value is None:
        return None
    if isinstance(value, bool):
        return int(value)
    if isinstance(value, int):
        return number_count(value)
    if isinstance(value, float):
        return number_count(int(value)) if value == int(value) else value
    text = str(value).strip()
    if not text:
        return None
    if re.fullmatch(r"[+-]?\d+", text):
        return number_count(int(text))
    if _NUM_LIKE_RE.fullmatch(text):
        try:
            dec = Decimal(text)
            if dec == dec.to_integral_value():
                return number_count(int(dec))
            return float(dec)
        except (InvalidOperation, ValueError, OverflowError):
            return text
    return text


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


def _semantic_leaf(value: Any) -> Any:
    """Normalize a single leaf for payload/state equality (int/str num, plain digits)."""
    if value is None:
        return None
    if isinstance(value, bool):
        return int(value)
    if isinstance(value, int):
        return value
    if isinstance(value, float):
        if value == int(value):
            try:
                return int(value)
            except (OverflowError, ValueError):
                return as_int_like(value)
        return value
    text = str(value).strip()
    if not text:
        return ""
    if re.fullmatch(r"[+-]?\d+", text):
        try:
            return int(text)
        except (TypeError, ValueError, OverflowError):
            return text
    if _NUM_LIKE_RE.fullmatch(text):
        try:
            dec = Decimal(text)
            if dec == dec.to_integral_value():
                return int(dec)
            return float(dec)
        except (InvalidOperation, ValueError, OverflowError):
            return text
    return text


def semantic_normalize(value: Any) -> Any:
    """Deep-normalize JSON-like values for semantic equality (not string equality).

    - dict keys sorted via dumps later; values recurse
    - list/tuple recurse in order
    - numeric strings / ints / float-integers unify to int when safe
    - empty / \"0\" for missing mix-elixir style records handled by callers
    """
    if isinstance(value, dict):
        return {str(k): semantic_normalize(v) for k, v in value.items()}
    if isinstance(value, (list, tuple)):
        return [semantic_normalize(v) for v in value]
    return _semantic_leaf(value)


def semantic_dumps(value: Any, *, ensure_ascii: bool = True) -> str:
    """Canonical JSON string for operation payload / state snapshots."""
    return json.dumps(
        semantic_normalize(value),
        ensure_ascii=ensure_ascii,
        sort_keys=True,
        separators=(",", ":"),
        default=str,
    )


def semantic_equal(left: Any, right: Any) -> bool:
    """True if two values are equal after semantic_normalize.

    Accepts raw objects or JSON strings (auto-parsed when possible).
    """

    def _coerce(v: Any) -> Any:
        if isinstance(v, (bytes, bytearray)):
            v = v.decode("utf-8", errors="replace")
        if isinstance(v, str):
            text = v.strip()
            if not text:
                return ""
            if text[0] in "{[":
                try:
                    return json.loads(text)
                except Exception:
                    return text
            return text
        return v

    return semantic_normalize(_coerce(left)) == semantic_normalize(_coerce(right))


def operation_payload_matches(stored: Any, expected: Any) -> bool:
    """Compare operation table payload column with newly built payload.

    Prefer this over ``str(previous[0]) == payload`` to avoid false
    ``state_changed`` when key order or int/str digits differ.
    """
    return semantic_equal(stored, expected)
