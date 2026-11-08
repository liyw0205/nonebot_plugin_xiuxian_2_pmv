"""历练的“每小时一次”窗口计算（时区安全）。

`training.last_time` 由 :mod:`event_planner` 用 ``SystemClock``（UTC aware）的
墙钟时间 ``strftime("%Y-%m-%d %H:%M:%S")`` 落库，读回来是 **naive** 的 UTC 墙钟；
而 handler 里的 ``runtime_clock.now()`` 是 **aware** 的 UTC。
直接相减会抛 ``TypeError: can't subtract offset-naive and offset-aware datetimes``，
这正是线上「开始历练 / 历练状态」崩溃的原因。

本模块把两侧统一到 UTC 再比较与相减，并把“下次可历练时间”换算成本地时区展示，
使玩家看到的钟点与他们真正能历练的时刻一致。
"""

from __future__ import annotations

from datetime import datetime, timedelta, timezone
from typing import Any


def to_utc(value: Any) -> datetime | None:
    """把库内 naive 墙钟（UTC）或 aware 时间统一成 aware UTC；坏值返回 None。"""
    if not isinstance(value, datetime):
        return None
    if value.tzinfo is None:
        return value.replace(tzinfo=timezone.utc)
    return value.astimezone(timezone.utc)


def training_hour_window(*, last_time: Any, now: Any) -> dict[str, Any]:
    """本小时窗口：是否同小时内、下次可历练时刻、还需等待几分钟。

    返回 ``in_same_hour``、``next_time``（aware UTC）、``next_time_local``（本地时区
    ``HH:MM``）与 ``wait_minutes``（永不为负）。任一时间为空或非 ``datetime`` 时，
    ``in_same_hour`` 为 False，玩家可立即历练。
    """
    last_utc = to_utc(last_time)
    now_utc = to_utc(now)
    if last_utc is None or now_utc is None:
        return {"in_same_hour": False, "next_time": None, "next_time_local": "现在", "wait_minutes": 0}
    in_same_hour = (
        last_utc.year == now_utc.year
        and last_utc.month == now_utc.month
        and last_utc.day == now_utc.day
        and last_utc.hour == now_utc.hour
    )
    if not in_same_hour:
        return {"in_same_hour": False, "next_time": None, "next_time_local": "现在", "wait_minutes": 0}
    next_time = (last_utc + timedelta(hours=1)).replace(minute=0, second=0, microsecond=0)
    wait_minutes = max(0, int((next_time - now_utc).total_seconds() // 60))
    return {
        "in_same_hour": True,
        "next_time": next_time,
        "next_time_local": next_time.astimezone().strftime("%H:%M"),
        "wait_minutes": wait_minutes,
    }


__all__ = ["to_utc", "training_hour_window"]
