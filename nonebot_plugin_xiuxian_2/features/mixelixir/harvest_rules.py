"""灵田收取规则（纯函数，无 I/O）。

`灵田收取` 原先在命令模块里内联三段规则：

* 成熟窗口硬编码成 ``{"time_cost": 23, "加速基数": 0.05}``，与收获仓储各写一份；
* 用裸 ``datetime.strptime`` 解析 ``mix_elixir_info."收取时间"``，缺行、``'0'``、
  或任何历史脏值都会抛 ``ValueError``，而 ``'0' != 0`` 让新号也会走进解析；
* 按 ``灵田数量 + 收取等级 + 传承/功法加成`` 逐次 ``choice`` 抽取药材并去重合并。

这些规则与入库、消息文案无关，放在这里以后展示、自动收取和命令入口共用同一份，
不会再各自漂移。数值语义与原实现逐字一致（时间成本 23 小时、加速基数 0.05）。
"""

from __future__ import annotations

from datetime import datetime, timedelta, timezone
from typing import Any, Callable, Iterable, Mapping

DEFAULT_TIME_COST_HOURS = 23.0
DEFAULT_SPEED_BASE = 0.05

# 候选药材为空时的兜底奖励（原实现硬编码）。
FALLBACK_HERB_ID = 3001
FALLBACK_HERB_NAME = "恒心草"

READY = "ready"
WAITING = "waiting"
INVALID = "invalid"


def as_int(value: Any, default: int = 0) -> int:
    """Tolerant integer coercion for historical text columns."""
    try:
        return int(value)
    except (TypeError, ValueError):
        try:
            return int(float(str(value)))
        except (TypeError, ValueError):
            return default


def normalize_now(value: datetime) -> datetime:
    """Normalize a comparison moment into the naive-UTC frame of 收取时间."""
    if value.tzinfo is None:
        return value
    return value.astimezone(timezone.utc).replace(tzinfo=None)


def is_never_harvested(value: Any) -> bool:
    """``收取时间`` never set: both the integer ``0`` and the text ``'0'`` count."""
    if value in (None, "", 0):
        return True
    return str(value).strip() in {"0", "0.0", "None", "none"}


def parse_harvest_time(value: Any) -> datetime | None:
    """Parse a stored 收取时间; ``None`` means the value cannot be trusted."""
    if isinstance(value, datetime):
        return normalize_now(value)
    if is_never_harvested(value):
        return None
    for fmt in ("%Y-%m-%d %H:%M:%S.%f", "%Y-%m-%d %H:%M:%S"):
        try:
            return datetime.strptime(str(value), fmt)
        except ValueError:
            continue
    return None


def maturity_interval_hours(
    time_cost_hours: float = DEFAULT_TIME_COST_HOURS,
    speed_base: float = DEFAULT_SPEED_BASE,
    herb_speed: Any = 0,
) -> float:
    """Maturity window: ``round(时间成本 × (1 - 加速基数 × 药材速度), 2)``."""
    speed = as_int(herb_speed)
    return round(float(time_cost_hours) * (1.0 - float(speed_base) * speed), 2)


def maturity_state(
    last_time: Any,
    now: datetime,
    *,
    herb_speed: Any = 0,
    time_cost_hours: float = DEFAULT_TIME_COST_HOURS,
    speed_base: float = DEFAULT_SPEED_BASE,
) -> tuple[str, float]:
    """Return ``(state, remaining_hours)`` for the ripening field.

    ``INVALID`` keeps the historical meaning of "还没有收取时间/值不可信" and lets
    the caller treat it exactly like a first harvest instead of raising.
    """
    parsed = parse_harvest_time(last_time)
    if parsed is None:
        return INVALID, 0.0
    moment = normalize_now(now)
    interval = maturity_interval_hours(time_cost_hours, speed_base, herb_speed)
    elapsed = round((moment - parsed).total_seconds() / 3600, 2)
    if elapsed >= interval:
        return READY, 0.0
    return WAITING, round(interval - elapsed, 2)


def harvest_count(
    field_count: Any = 0,
    harvest_level: Any = 0,
    impart_reap_per: Any = 0,
    reap_buff: Any = 0,
) -> int:
    """本次可收取数量：灵田数量 + 收取等级 + 传承加成 + 功法加成。"""
    return max(
        as_int(field_count) + as_int(harvest_level) + as_int(impart_reap_per) + as_int(reap_buff),
        0,
    )


def roll_rewards(
    candidate_ids: Iterable[Any],
    count: int,
    name_of: Callable[[int], str],
    rng: Any,
) -> tuple[tuple[int, str, int], ...]:
    """随机抽取 ``count`` 次药材并去重计数（无候选时给兜底恒心草）。"""
    candidates = [as_int(item_id) for item_id in candidate_ids]
    candidates = [item_id for item_id in candidates if item_id > 0]
    total = max(int(count), 0)
    if total <= 0:
        return ()
    if not candidates:
        return ((FALLBACK_HERB_ID, FALLBACK_HERB_NAME, total),)
    counts: dict[int, int] = {}
    for _ in range(total):
        item_id = int(rng.choice(candidates))
        counts[item_id] = counts.get(item_id, 0) + 1
    return tuple(
        (item_id, str(name_of(item_id)), quantity)
        for item_id, quantity in counts.items()
    )


def remaining_text(remaining_hours: float) -> str:
    """把剩余小时格式化成历史文案使用的 ``X小时Y分钟``。"""
    hours = int(remaining_hours)
    minutes = int((float(remaining_hours) - hours) * 60)
    return f"{hours}小时{minutes}分钟"


def field_status_text(
    last_time: Any,
    now: datetime,
    *,
    time_cost_hours: float = DEFAULT_TIME_COST_HOURS,
) -> str:
    """``日常`` 灵田行文案：已成熟 / 剩余 X时Y分后 / 未开启。

    ``收取时间`` 存的是 naive UTC 文本，而 ``runtime_clock.now()`` 是 aware UTC；
    旧实现直接 ``strptime`` 后与 aware 时间相减，线上抛
    ``TypeError: can't subtract offset-naive and offset-aware datetimes``。
    这里复用同一份解析/归一函数（``parse_harvest_time`` / ``normalize_now``），
    缺值、``'0'`` 与不可信脏值返回「未开启」，绝不凭空显示已成熟。
    """
    parsed = parse_harvest_time(last_time)
    if parsed is None:
        return "⬜ 未开启"
    moment = normalize_now(now)
    next_collect = parsed + timedelta(hours=float(time_cost_hours))
    if moment >= next_collect:
        return "🌱 已成熟"
    time_left = next_collect - moment
    hours_left = time_left.seconds // 3600
    minutes_left = (time_left.seconds % 3600) // 60
    return f"⏳ {hours_left}时{minutes_left}分后"


def normalize_rewards(
    rewards: Iterable[Any],
) -> tuple[tuple[int, str, int], ...]:
    """承载 payload 的 3 元组形状；同时容忍 mapping 形状（来自回执重放）。"""
    normalized: list[tuple[int, str, int]] = []
    for reward in rewards:
        if isinstance(reward, Mapping):
            normalized.append(
                (
                    as_int(reward.get("item_id") or reward.get("goods_id")),
                    str(reward.get("name") or reward.get("goods_name") or ""),
                    as_int(reward.get("quantity") or reward.get("amount")),
                )
            )
        else:
            item_id, name, quantity = reward
            normalized.append((as_int(item_id), str(name), as_int(quantity)))
    return tuple(normalized)


__all__ = [
    "DEFAULT_SPEED_BASE",
    "DEFAULT_TIME_COST_HOURS",
    "FALLBACK_HERB_ID",
    "FALLBACK_HERB_NAME",
    "INVALID",
    "READY",
    "WAITING",
    "as_int",
    "field_status_text",
    "harvest_count",
    "is_never_harvested",
    "maturity_interval_hours",
    "maturity_state",
    "normalize_now",
    "normalize_rewards",
    "parse_harvest_time",
    "remaining_text",
    "roll_rewards",
]
