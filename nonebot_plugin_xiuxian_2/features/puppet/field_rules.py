"""灵田成熟规则（纯函数）。

`mix_elixir_info."收取时间"` 以 ``%Y-%m-%d %H:%M:%S`` 的 naive UTC 文本保存，
而运行时钟 ``SystemClock.now()`` 返回 aware UTC。收获仓储和玩家读模型必须共用
这里的同一条时间解析与成熟窗口规则，否则展示的「还需多久」和实际能否收取会各自漂移。
"""

from __future__ import annotations

from datetime import datetime, timezone
from typing import Any
from ...core.numeric import as_int_like

DEFAULT_TIME_COST_HOURS = 23.0
DEFAULT_SPEED_BASE = 0.05

READY = "ready"
WAITING = "waiting"
INVALID = "invalid"


def normalize_now(value: datetime) -> datetime:
    """把比较时刻归一为 ``收取时间`` 使用的 naive UTC 帧。"""
    if value.tzinfo is None:
        return value
    return value.astimezone(timezone.utc).replace(tzinfo=None)


def parse_harvest_time(value: Any) -> datetime | None:
    """解析已保存的 ``收取时间``；``None`` 表示该值不可信。"""
    if isinstance(value, datetime):
        return normalize_now(value)
    if value in (None, "", 0, "0"):
        return None
    for fmt in ("%Y-%m-%d %H:%M:%S.%f", "%Y-%m-%d %H:%M:%S"):
        try:
            return datetime.strptime(str(value), fmt)
        except ValueError:
            continue
    return None


def as_int(value: Any, default: int = 0) -> int:
    return as_int_like(value, default)


def maturity_interval_hours(
    time_cost_hours: float = DEFAULT_TIME_COST_HOURS,
    speed_base: float = DEFAULT_SPEED_BASE,
    herb_speed: int = 0,
) -> float:
    """成熟窗口：``时间成本 × (1 - 加速基数 × 药材速度)``，下限 0。"""
    return max(
        float(time_cost_hours) * (1.0 - float(speed_base) * float(herb_speed)),
        0.0,
    )


def maturity_state(
    last_time: Any,
    now: datetime,
    *,
    time_cost_hours: float = DEFAULT_TIME_COST_HOURS,
    speed_base: float = DEFAULT_SPEED_BASE,
    herb_speed: int = 0,
) -> tuple[str, float]:
    """返回 ``(状态, 剩余小时)``；状态为 ready / waiting / invalid。"""
    parsed = parse_harvest_time(last_time)
    if parsed is None:
        return INVALID, 0.0
    moment = normalize_now(now)
    interval = maturity_interval_hours(time_cost_hours, speed_base, herb_speed)
    elapsed = (moment - parsed).total_seconds() / 3600
    if elapsed >= interval:
        return READY, 0.0
    return WAITING, max(interval - elapsed, 0.0)


__all__ = [
    "DEFAULT_SPEED_BASE",
    "DEFAULT_TIME_COST_HOURS",
    "INVALID",
    "READY",
    "WAITING",
    "as_int",
    "maturity_interval_hours",
    "maturity_state",
    "normalize_now",
    "parse_harvest_time",
]
