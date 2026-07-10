from __future__ import annotations

from datetime import date, datetime
from typing import Optional, Union

PeriodNow = Optional[Union[date, datetime]]

__all__ = [
    "get_daily_key",
    "get_weekly_key",
    "get_monthly_key",
    "get_season_key",
    "format_duration_compact",
    "format_duration_full",
    "format_remaining_time",
]


def _duration_parts(seconds: float | int) -> tuple[int, int, int, int]:
    total = max(0, int(seconds))
    days, remainder = divmod(total, 86400)
    hours, remainder = divmod(remainder, 3600)
    minutes, secs = divmod(remainder, 60)
    return days, hours, minutes, secs


def format_duration_full(seconds: float | int, *, zero: str = "0秒") -> str:
    if seconds <= 0:
        return zero
    days, hours, minutes, secs = _duration_parts(seconds)
    return f"{days}天{hours}小时{minutes}分{secs}秒"


def format_duration_compact(seconds: float | int, *, zero: str = "0秒") -> str:
    if seconds <= 0:
        return zero
    parts = _duration_parts(seconds)
    labels = ("天", "小时", "分", "秒")
    first = next((index for index, value in enumerate(parts) if value), 3)
    return "".join(
        f"{value}{label}" for value, label in zip(parts[first:], labels[first:])
    )


def format_remaining_time(seconds: float | int) -> str:
    return format_duration_compact(seconds, zero="已可用")


def _to_date(now: PeriodNow = None) -> date:
    """Normalize accepted date inputs to a date object."""
    if now is None:
        return date.today()
    if isinstance(now, datetime):
        return now.date()
    if isinstance(now, date):
        return now
    raise TypeError("now must be None, datetime.datetime, or datetime.date")


def get_daily_key(now: PeriodNow = None) -> str:
    """Return the daily period key in YYYY-MM-DD format."""
    current = _to_date(now)
    return f"{current.year:04d}-{current.month:02d}-{current.day:02d}"


def get_weekly_key(now: PeriodNow = None) -> str:
    """Return the ISO weekly period key in YYYY-Www format."""
    current = _to_date(now)
    iso_year, iso_week, _ = current.isocalendar()
    return f"{iso_year:04d}-W{iso_week:02d}"


def get_monthly_key(now: PeriodNow = None) -> str:
    """Return the monthly period key in YYYY-MM format."""
    current = _to_date(now)
    return f"{current.year:04d}-{current.month:02d}"


def get_season_key(now: PeriodNow = None, mode: str = "monthly") -> str:
    """Return a period key for the configured season mode."""
    normalized_mode = mode.strip().lower()
    if normalized_mode == "weekly":
        return get_weekly_key(now)
    if normalized_mode == "quarterly":
        current = _to_date(now)
        quarter = (current.month - 1) // 3 + 1
        return f"{current.year:04d}-Q{quarter}"
    return get_monthly_key(now)
