from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
from typing import Any, Callable, Mapping, Sequence


def parse_tianti_time(value: Any) -> datetime | None:
    if not value:
        return None
    if isinstance(value, datetime):
        return value
    for fmt in ("%Y-%m-%d %H:%M:%S", "%Y-%m-%d %H:%M:%S.%f"):
        try:
            return datetime.strptime(str(value), fmt)
        except ValueError:
            continue
    return None


def calc_qiaoxue_bonus(data: Mapping[str, Any]) -> tuple[float, float]:
    base_ratio = 0.0
    gain_pct = 0.0
    for item in data.get("opened_qiaoxue_detail", []) or []:
        if not isinstance(item, Mapping):
            continue
        effect_type = item.get("effect_type")
        effect_value = float(item.get("effect_value", 0))
        if effect_type == "base_per_min_ratio":
            base_ratio += effect_value
        elif effect_type == "hp_gain_pct":
            gain_pct += effect_value
    return base_ratio, gain_pct


def get_active_medicine_bath(data: Mapping[str, Any], now: datetime) -> dict[str, Any] | None:
    end_time = parse_tianti_time(data.get("medicine_end_time"))
    if end_time is None or now > end_time:
        return None
    try:
        effect = float(data.get("medicine_effect", 0) or 0)
    except (TypeError, ValueError, OverflowError):
        effect = 0.0
    if effect <= 1:
        return None
    return {
        "name": data.get("medicine_name") or "未知药材",
        "effect": effect,
        "end_time": end_time,
    }


def get_sect_fairyland_bonus(level: int) -> float:
    try:
        normalized_level = int(level or 0)
    except (TypeError, ValueError, OverflowError):
        normalized_level = 0
    return max(0, min(normalized_level, 10)) * 0.05


@dataclass(frozen=True)
class BathSlotView:
    """One configured medicine-bath time window, as shown to players."""

    name: str
    start: int
    end: int
    herbs: tuple[str, ...]
    current: bool
    remaining_minutes: int


@dataclass(frozen=True)
class BathStatus:
    """Three-state view of the player's medicine bath."""

    state: str  # "active" | "expired" | "none"
    name: str = ""
    effect: float = 0.0
    end_time: datetime | None = None
    remaining_minutes: int = 0

    def __bool__(self) -> bool:
        return self.state == "active"


def _as_hour(value: Any) -> int | None:
    try:
        hour = int(value)
    except (TypeError, ValueError, OverflowError):
        return None
    if 0 <= hour <= 24:
        return hour
    return None


def bath_slot_remaining(slot: Mapping[str, Any], now: datetime) -> int:
    """Minutes left in ``slot`` at ``now``; 0 when the slot is not current.

    Wrapping windows (``end <= start``, e.g. 23 -> 5) are handled by comparing
    the hour against both halves of the window.
    """
    start = _as_hour(slot.get("start"))
    end = _as_hour(slot.get("end"))
    if start is None or end is None or not isinstance(now, datetime):
        return 0
    hour = int(now.hour)
    in_window = (start <= hour < end) if end > start else (hour >= start or hour < end)
    if not in_window:
        return 0
    minutes_into_day = now.hour * 60 + now.minute
    # A wrapping window ends on the next calendar day in its pre-midnight half
    # and on the same day in its post-midnight half.
    if end > start or hour < end:
        window_end = end * 60
    else:
        window_end = (24 + end) * 60
    return max(0, window_end - minutes_into_day)


def bath_schedule(
    config: Sequence[Mapping[str, Any]] | None,
    now: datetime,
    *,
    herb_limit: int = 10,
) -> tuple[BathSlotView, ...]:
    """Read the whole configured bath schedule, flagging the current window.

    Slot order follows ``config``; malformed entries are skipped rather than
    raising, so a damaged config degrades to a shorter (or empty) schedule.
    """
    views: list[BathSlotView] = []
    for entry in config or ():
        if not isinstance(entry, Mapping):
            continue
        name = str(entry.get("name") or "").strip()
        start = _as_hour(entry.get("start"))
        end = _as_hour(entry.get("end"))
        if not name or start is None or end is None:
            continue
        raw_herbs = entry.get("herbs") or ()
        if isinstance(raw_herbs, (str, bytes)) or not isinstance(raw_herbs, (list, tuple, set, frozenset)):
            herbs: tuple[str, ...] = ()
        else:
            herbs = tuple(sorted({str(item) for item in raw_herbs if str(item)}))[:herb_limit]
        remaining = bath_slot_remaining(entry, now)
        views.append(
            BathSlotView(
                name=name,
                start=start,
                end=end,
                herbs=herbs,
                current=remaining > 0,
                remaining_minutes=remaining,
            )
        )
    return tuple(views)


def bath_remaining_text(end_time: Any, now: datetime) -> str:
    """``剩余 N 分钟`` for an active window; empty when it already ended."""
    end_time = parse_tianti_time(end_time)
    if end_time is None or not isinstance(now, datetime):
        return ""
    try:
        minutes = int((end_time - now).total_seconds() // 60)
    except (TypeError, ValueError, OverflowError, OSError):
        return ""
    if minutes <= 0:
        return ""
    return f"剩余 {minutes} 分钟"


def medicine_bath_active(data: Mapping[str, Any], now: datetime) -> dict[str, Any] | None:
    """Single source of truth for "is this bath still in effect?"."""
    return get_active_medicine_bath(data, now)


def bath_status(data: Mapping[str, Any] | None, now: datetime) -> BathStatus:
    """Three-state bath status, so an expired bath is not shown as 'never used'."""
    if not isinstance(data, Mapping):
        return BathStatus("none")
    active = get_active_medicine_bath(data, now)
    if active is not None:
        return BathStatus(
            state="active",
            name=str(active.get("name") or "未知药材"),
            effect=float(active.get("effect") or 0.0),
            end_time=active.get("end_time"),
            remaining_minutes=_remaining_minutes(active.get("end_time"), now),
        )
    if data.get("medicine_end_time"):
        return BathStatus(
            state="expired",
            name=str(data.get("medicine_name") or "未知药材"),
            effect=_safe_effect(data.get("medicine_effect")),
            end_time=parse_tianti_time(data.get("medicine_end_time")),
        )
    return BathStatus("none")


def _safe_effect(value: Any) -> float:
    try:
        return float(value or 0)
    except (TypeError, ValueError, OverflowError):
        return 0.0


def _remaining_minutes(end_time: Any, now: datetime) -> int:
    end_time = parse_tianti_time(end_time)
    if end_time is None or not isinstance(now, datetime):
        return 0
    try:
        return max(0, int((end_time - now).total_seconds() // 60))
    except (TypeError, ValueError, OverflowError, OSError):
        return 0


def render_bath_schedule(views: Sequence[BathSlotView]) -> tuple[str, ...]:
    """Human lines for the whole schedule; the current window is marked."""
    lines: list[str] = []
    for view in views:
        marker = "◇" if view.current else "·"
        window = f"{view.start:02d}:00-{view.end:02d}:00"
        herbs = "、".join(view.herbs) if view.herbs else "（未配置）"
        line = f"{marker} {view.name}（{window}）：{herbs}"
        if view.current:
            hours, minutes = divmod(view.remaining_minutes, 60)
            if hours and minutes:
                remaining = f"{hours} 小时 {minutes} 分钟"
            elif hours:
                remaining = f"{hours} 小时"
            else:
                remaining = f"{minutes} 分钟"
            line += f"\n  本时段剩余 {remaining}"
        lines.append(line)
    return tuple(lines)


def render_bath_status(status: BathStatus, *, percent: Callable[[float], str] | None = None) -> str:
    """Three-state bath line for 我的炼体; ``none`` keeps the historical wording."""
    if status.state == "active":
        effect = percent(status.effect) if percent else f"{status.effect * 100:.2f}"
        tail = ""
        if status.end_time is not None:
            tail = f"（有效至{status.end_time.strftime('%H:%M')}，剩余 {status.remaining_minutes} 分钟）"
        return f"\n药浴：{status.name}，结算效果{effect}%{tail}"
    if status.state == "expired":
        when = status.end_time.strftime("%m-%d %H:%M") if status.end_time is not None else "未知时间"
        return f"\n药浴：上次药浴已结束（{status.name}，到期 {when}）"
    return "\n药浴：无"


def render_bath_remaining_suffix(status: BathStatus) -> str:
    """``（当前药浴剩余 N 分钟）`` for the refusal branch; empty when inactive."""
    if status.state != "active" or status.remaining_minutes <= 0:
        return ""
    return f"（当前药浴剩余 {status.remaining_minutes} 分钟）"


def calculate_tianti_gain_rate(
    data: Mapping[str, Any],
    *,
    base_per_min: int,
    now: datetime,
    sect_fairyland_level: int = 0,
    spirit_vein_multiplier: float = 1.0,
) -> dict[str, Any]:
    base_per_min = int(base_per_min)
    base_ratio, gain_pct = calc_qiaoxue_bonus(data)
    real_per_min = int(base_per_min * (1 + base_ratio))
    bath = get_active_medicine_bath(data, now)
    bath_effect = bath["effect"] if bath else 1.0
    sect_bonus = get_sect_fairyland_bonus(sect_fairyland_level)
    spirit_vein_multiplier = float(spirit_vein_multiplier)
    per_min = int(real_per_min * (1 + gain_pct) * bath_effect * (1 + sect_bonus) * spirit_vein_multiplier)
    return {
        "base_per_min": base_per_min,
        "base_ratio": base_ratio,
        "gain_pct": gain_pct,
        "bath": bath,
        "bath_effect": bath_effect,
        "sect_bonus": sect_bonus,
        "spirit_vein_bonus": spirit_vein_multiplier - 1,
        "per_min": per_min,
        "efficiency": (per_min / base_per_min) if base_per_min > 0 else 0,
    }


__all__ = [
    "BathSlotView",
    "BathStatus",
    "bath_remaining_text",
    "bath_schedule",
    "bath_slot_remaining",
    "bath_status",
    "calc_qiaoxue_bonus",
    "calculate_tianti_gain_rate",
    "get_active_medicine_bath",
    "get_sect_fairyland_bonus",
    "medicine_bath_active",
    "parse_tianti_time",
]
