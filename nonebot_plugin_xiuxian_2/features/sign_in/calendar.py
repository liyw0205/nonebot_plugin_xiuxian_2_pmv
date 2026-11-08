"""Pure sign-in calendar and streak rendering.

QQ 原生 Markdown 支持表格（见 `docs/qq-markdown-reference.md`），所以日历按
周一→周日七列排成一张真实的月度表格：格子边界由表头与分隔行表达，不靠空格补齐，
手机与非等宽字体下都不会散列。
"""

from __future__ import annotations

import calendar
from datetime import date, timedelta
from typing import Iterable


WEEKDAYS = ("一", "二", "三", "四", "五", "六", "日")

SIGNED_MARK = "✓"
TODAY_MARK = "·"
EMPTY_CELL = "—"


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


def _cell(moment: date, day: int, signed: set[str]) -> str:
    value = date(moment.year, moment.month, day).isoformat()
    if value in signed:
        return f"{day}{SIGNED_MARK}"
    if value == moment.isoformat():
        return f"{day}{TODAY_MARK}"
    return str(day)


def render_sign_in_calendar(
    today: str | date, signed_dates: Iterable[str | date]
) -> tuple[str, ...]:
    """Render the current month as a real seven-column Markdown table."""

    moment = _as_date(today)
    signed = {_as_date(value).isoformat() for value in signed_dates}
    days_in_month = calendar.monthrange(moment.year, moment.month)[1]
    month_days = [
        date(moment.year, moment.month, day).isoformat()
        for day in range(1, days_in_month + 1)
    ]
    month_signed = [value for value in month_days if value in signed]

    lines = [
        f"**签到日历 {moment.year}-{moment.month:02d}**",
        "",
        "| " + " | ".join(WEEKDAYS) + " |",
        "| " + " | ".join(["---"] * len(WEEKDAYS)) + " |",
    ]
    for week in calendar.Calendar(firstweekday=0).monthdayscalendar(
        moment.year, moment.month
    ):
        cells = [_cell(moment, day, signed) if day else EMPTY_CELL for day in week]
        lines.append("| " + " | ".join(cells) + " |")
    lines.append("")
    lines.append(
        f"{SIGNED_MARK} 已签｜{TODAY_MARK} 今日｜{EMPTY_CELL} 非本月"
        f"｜本月已签 {len(month_signed)} 天"
        f"｜连续 {signed_streak(signed, moment)} 天"
    )
    return tuple(lines)


__all__ = ["render_sign_in_calendar", "signed_streak"]
