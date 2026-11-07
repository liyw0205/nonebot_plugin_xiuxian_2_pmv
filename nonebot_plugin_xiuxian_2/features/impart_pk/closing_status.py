"""虚神界闭关状态的只读渲染（纯函数：无 I/O、无 NoneBot）。

`虚神界信息` 过去只展示修炼时间/效率/次数，玩家进入闭关（`user_cd.type=4`）后
无法从面板得知「是否在闭关、已入定多久、还剩多少祝福时间、离本境修为上限还有多远」，
只能盲发 `虚神界出关` 再看结算文本。这里把该状态压成一个纯函数，命令适配器只负责
取 `user_cd`、`impart_data.exp_day`、当前修为与本境上限。

口径与结算方保持一致：
- 已入定分钟数复用 `elapsed_minutes_from_cd_time(..., on_error=0)`，坏/空时间记 0；
- 祝福余额取 `xiuxian_impart.exp_day`（与 `虚神界出关` 的 `available_exp_day` 同源），
  负数按 0 处理；
- 修为上限余量 = `max_exp - 当前修为`，下限 0；`max_exp<=0`（配置缺失）时不显示该行，
  避免给出「余量 0」的误导结论。
"""

from __future__ import annotations

from datetime import datetime
from typing import Any, Mapping

from ...xiuxian.xiuxian_utils.cd_time import elapsed_minutes_from_cd_time


def _int(value: Any, default: int = 0) -> int:
    try:
        return int(value)
    except (TypeError, ValueError, OverflowError):
        return default


def closing_status_lines(
    cooldown: Mapping[str, Any] | None,
    *,
    exp_day: Any,
    current_exp: Any,
    max_exp: Any,
    now: datetime | None = None,
) -> tuple[str, ...]:
    """返回闭关状态行；未处于闭关（type != 4）时返回空元组。"""
    if not isinstance(cooldown, Mapping) or _int(cooldown.get("type")) != 4:
        return ()

    elapsed = _int(
        elapsed_minutes_from_cd_time(cooldown.get("create_time"), now=now, on_error=0)
    )
    lines = [
        f"虚神界闭关中（已入定 {max(0, elapsed)} 分钟）",
        f"今日祝福余额：{max(0, _int(exp_day))} 分钟",
    ]
    limit = _int(max_exp)
    if limit > 0:
        lines.append(f"本境修为上限余量：{max(0, limit - _int(current_exp))}")
    lines.append("发送【虚神界出关】结算本次闭关")
    return tuple(lines)


def closing_settlement_lines(
    *,
    blessing_cost: Any,
    exp_day_remaining: Any,
    minutes: Any = 0,
) -> tuple[str, ...]:
    """出关回执的祝福消耗/余额段（纯函数，供 applied 与 duplicate 两条路径共用）。

    `ImpartClosingSettlementResult` 一直携带 `blessing_cost` 与 `exp_day_remaining`，
    但回执只渲染分钟数与修为，玩家看不到本次消耗了多少祝福、还剩多少。这里把两者
    压成固定文案：

    - 有消耗：`本次消耗虚神界祝福 N 分钟，剩余 M 分钟`
    - 无消耗（`blessing_cost <= 0`，例如祝福已耗尽或本次全为普通修炼）：
      `本次未消耗虚神界祝福，剩余 M 分钟`

    非数字/负值按 0 处理；旧回执缺这两个字段时由调用方决定是否追加（返回空元组
    以外的内容只在调用方显式传入时出现）。
    """
    remaining = max(0, _int(exp_day_remaining))
    cost = max(0, _int(blessing_cost))
    suffix = f"（本次闭关 {max(0, _int(minutes))} 分钟）" if _int(minutes) > 0 else ""
    if cost > 0:
        return (f"本次消耗虚神界祝福 {cost} 分钟，剩余 {remaining} 分钟{suffix}",)
    return (f"本次未消耗虚神界祝福，剩余 {remaining} 分钟{suffix}",)


__all__ = ["closing_settlement_lines", "closing_status_lines"]
