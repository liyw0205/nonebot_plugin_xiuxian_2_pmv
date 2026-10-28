"""Pure sign-in calendar and streak rendering."""

from __future__ import annotations

import calendar
from datetime import date, timedelta
from typing import Iterable


WEEKDAYS = ("一", "二", "三", "四", "五", "六", "日")


def _as_date(value: str | date) -> date:
    return value if isinstance(value, date) else date.fromisoformat(str(value))


def signed_streak(signed_dates: Iterable[str | date], today: str | date) -> int:
    """Count consecutive signed days ending today (or yesterday if today is blank)."""

    signed = {_as_date(value).isoformat() for value in signed_dates}
    cursor = _as_date(today)
    if cursor.isoformat() not in signed:
        cursor -= timedelta(days=1)
    count = 0
    while cursor.isoformat() in signed:
        count += 1
        cursor -= timedelta(days=1)
    return count


def render_sign_in_calendar(
    today: str | date, signed_dates: Iterable[str | date]
) -> tuple[str, ...]:
    """Render the current month as a compact seven-column calendar."""

    moment = _as_date(today)
    signed = {_as_date(value).isoformat() for value in signed_dates}
    month_days = [
        day.isoformat()
        for day in (
            date(moment.year, moment.month, day)
            for day in range(1, calendar.monthrange(moment.year, moment.month)[1] + 1)
        )
    ]
    month_signed = [value for value in month_days if value in signed]
    lines = [
        f"【签到日历 {moment.year}-{moment.month:02d}】",
        "  ".join(WEEKDAYS),
    ]
    for week in calendar.Calendar(firstweekday=0).monthdayscalendar(
        moment.year, moment.month
    ):
        cells: list[str] = []
        for day in week:
            if day == 0:
                cells.append("    ")
                continue
            value = date(moment.year, moment.month, day).isoformat()
            if value in signed:
                mark = "✓"
            elif value == moment.isoformat():
                mark = "·"
            else:
                mark = " "
            cells.append(f"{day:2d}{mark}")
        lines.append(" ".join(cells).rstrip())
    lines.append(
        f"本月已签 {len(month_signed)} 天｜连续 {signed_streak(signed, moment)} 天"
    )
    return tuple(lines)


__all__ = ["render_sign_in_calendar", "signed_streak"]
