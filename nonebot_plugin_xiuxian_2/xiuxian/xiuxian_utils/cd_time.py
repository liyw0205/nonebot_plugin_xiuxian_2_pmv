"""Safe user_cd / settlement timestamp helpers.

Bad or legacy create_time values ('0', '', None, unparseable) must not trap
players who are already in the correct type state (闭关/悬赏/秘境/虚神界…).
Duration math falls back to a default; session identity prefers type and only
matches create_time when both sides are real timestamps.
"""

from __future__ import annotations

from datetime import datetime, timezone
from typing import Any

_BLANK = frozenset({"", "0", "none", "null", "nil", "undefined"})
_FORMATS = (
    "%Y-%m-%d %H:%M:%S.%f",
    "%Y-%m-%d %H:%M:%S",
    "%Y-%m-%d %H:%M",
    "%Y/%m/%d %H:%M:%S.%f",
    "%Y/%m/%d %H:%M:%S",
)


def is_blank_cd_time(value: Any) -> bool:
    if value is None:
        return True
    if isinstance(value, datetime):
        return False
    text = str(value).strip()
    if not text:
        return True
    return text.lower() in _BLANK


def parse_cd_datetime(value: Any, *, default: datetime | None = None) -> datetime | None:
    """Parse create_time / scheduled_time; return default on blank/bad input."""
    if isinstance(value, datetime):
        return value
    if is_blank_cd_time(value):
        return default
    text = str(value).strip().replace("Z", "+00:00")
    for fmt in _FORMATS:
        try:
            return datetime.strptime(text, fmt)
        except ValueError:
            continue
    try:
        return datetime.fromisoformat(text)
    except ValueError:
        return default


def elapsed_minutes_from_cd_time(
    value: Any,
    *,
    now: datetime | None = None,
    on_error: int = 0,
) -> int:
    """Minutes since create_time. Bad/blank time → on_error (default 0)."""
    started = parse_cd_datetime(value)
    if started is None:
        return max(0, int(on_error))
    current = now or datetime.now()
    if started.tzinfo is not None:
        if current.tzinfo is None:
            current = current.replace(tzinfo=timezone.utc).astimezone(started.tzinfo)
        else:
            current = current.astimezone(started.tzinfo)
    else:
        if current.tzinfo is not None:
            current = current.replace(tzinfo=None)
    try:
        seconds = (current - started).total_seconds()
    except Exception:
        return max(0, int(on_error))
    return max(0, int(seconds // 60))


def cd_time_matches(actual: Any, expected: Any) -> bool:
    """True when timestamps agree, or either side is blank/garbage (don't block)."""
    if is_blank_cd_time(actual) or is_blank_cd_time(expected):
        return True
    return str(actual).strip() == str(expected).strip()


def normalize_cd_time_token(value: Any) -> str:
    """Stable token for payloads: blank/bad → '0'."""
    if is_blank_cd_time(value):
        return "0"
    return str(value).strip()
