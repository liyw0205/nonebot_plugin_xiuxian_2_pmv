"""Pure Guishi order lifecycle rules.

The Guishi buy-order (求购) text promises "次日 8 点自动取消订单，并退还未购得物品的
灵石", but the order table carries no timestamp. The durable creation anchor is the
``guishi_order_create_operations.created_at`` row written in the same transaction as
the order itself, so the deadline is derived from that row instead of a new column.

Everything here is pure: no database, no NoneBot, no clock reads. The caller passes
the instant it already has, so the same rule serves the scheduled sweep and a
player-triggered convergence without a second notion of "now".
"""

from __future__ import annotations

from datetime import datetime, timedelta, timezone

# The hour the Guishi sell window closes; the buy-order promise quotes the same hour.
DEFAULT_GUISHI_END_HOUR = 8

# SQLite ``CURRENT_TIMESTAMP`` writes UTC without an offset.
_SQLITE_TIMESTAMP_FORMATS = (
    "%Y-%m-%d %H:%M:%S.%f",
    "%Y-%m-%d %H:%M:%S",
    "%Y-%m-%dT%H:%M:%S.%f",
    "%Y-%m-%dT%H:%M:%S",
)


def parse_created_at(value: object) -> datetime | None:
    """Return an aware UTC instant for a stored operation timestamp.

    Missing or malformed values return ``None`` so callers can fail safe: an order
    whose creation time cannot be proven is never auto-cancelled.
    """

    if value is None:
        return None
    if isinstance(value, datetime):
        if value.tzinfo is None:
            return value.replace(tzinfo=timezone.utc)
        return value.astimezone(timezone.utc)
    text = str(value).strip()
    if not text:
        return None
    normalized = text.replace("Z", "+00:00") if text.endswith("Z") else text
    try:
        parsed = datetime.fromisoformat(normalized)
    except ValueError:
        parsed = None
    if parsed is None:
        for fmt in _SQLITE_TIMESTAMP_FORMATS:
            try:
                parsed = datetime.strptime(text, fmt)
            except ValueError:
                continue
            break
    if parsed is None:
        return None
    if parsed.tzinfo is None:
        return parsed.replace(tzinfo=timezone.utc)
    return parsed.astimezone(timezone.utc)


def _compare_frames(
    deadline: datetime, now: datetime
) -> tuple[datetime, datetime]:
    """Drop tzinfo from both sides when the caller mixed naive and aware instants."""

    if (deadline.tzinfo is None) != (now.tzinfo is None):
        return deadline.replace(tzinfo=None), now.replace(tzinfo=None)
    return deadline, now


def qiugou_deadline(
    created_at: datetime | None, *, end_hour: int = DEFAULT_GUISHI_END_HOUR
) -> datetime | None:
    """Deadline of a buy order: ``end_hour`` on the day AFTER it was created."""

    if not isinstance(created_at, datetime):
        return None
    day = created_at.date() + timedelta(days=1)
    return datetime(
        day.year, day.month, day.day, int(end_hour), 0, 0, tzinfo=created_at.tzinfo
    )


def qiugou_is_expired(
    created_at: datetime | None,
    now: datetime,
    *,
    end_hour: int = DEFAULT_GUISHI_END_HOUR,
) -> bool:
    """True only when the deadline is provably reached; unknown anchors never expire."""

    deadline = qiugou_deadline(created_at, end_hour=end_hour)
    if deadline is None or not isinstance(now, datetime):
        return False
    deadline, now = _compare_frames(deadline, now)
    return now >= deadline


def qiugou_remaining_seconds(
    created_at: datetime | None,
    now: datetime,
    *,
    end_hour: int = DEFAULT_GUISHI_END_HOUR,
) -> int:
    """Seconds left before the buy order expires; 0 when expired or unknown."""

    deadline = qiugou_deadline(created_at, end_hour=end_hour)
    if deadline is None or not isinstance(now, datetime):
        return 0
    deadline, now = _compare_frames(deadline, now)
    remaining = (deadline - now).total_seconds()
    return max(int(remaining), 0)


__all__ = [
    "DEFAULT_GUISHI_END_HOUR",
    "parse_created_at",
    "qiugou_deadline",
    "qiugou_is_expired",
    "qiugou_remaining_seconds",
]
