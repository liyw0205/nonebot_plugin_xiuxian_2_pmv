from __future__ import annotations

from collections.abc import Sequence
from dataclasses import dataclass
from datetime import date, timedelta


@dataclass(frozen=True)
class ActivitySignDayView:
    sign_date: str
    day_index: int = 0
    reward_status: str = ""


def _sign_date(value: str) -> date | None:
    try:
        return date.fromisoformat(str(value)[:10])
    except (TypeError, ValueError):
        return None


def render_sign_calendar(
    days: Sequence[ActivitySignDayView], *, today: date, limit: int = 7
) -> str:
    size = max(int(limit), 0)
    if size == 0 or not days:
        return ""
    signed = {
        parsed
        for row in days
        if (parsed := _sign_date(row.sign_date)) is not None
    }
    if not signed:
        return ""
    lines = ["", "【最近活动签到】"]
    for offset in range(size - 1, -1, -1):
        day = today - timedelta(days=offset)
        lines.append(f"{day.strftime('%m-%d')} {'已签' if day in signed else '未签'}")
    return "\n".join(lines)


__all__ = ["ActivitySignDayView", "render_sign_calendar"]
