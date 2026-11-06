"""悬赏令玩家侧纯渲染（无 I/O、无 NoneBot）。

两处反馈此前缺失：
- `悬赏令查看`（状态 3）只显示剩余时间与任务列表，不显示 `user_cd`/`user_xiuxian.work_num`
  里的剩余刷新次数，玩家要再发一次刷新才知道；
- 结算写入会把修为截到本境上限（`min(exp_gain, max_exp - 当前修为)`），但回执只报
  实际值，不解释为什么比悬赏上写的基础报酬少。
"""

from __future__ import annotations

from typing import Any


def _int(value: Any, default: int = 0) -> int:
    try:
        return int(float(value))
    except (TypeError, ValueError, OverflowError):
        return default


def refresh_count_line(work_num: Any, total: Any) -> str:
    """剩余刷新次数行；缺失/坏值回退到配置总数，并夹在 [0, total]。"""
    limit = max(0, _int(total))
    value = _int(work_num, default=limit)
    return f"今日剩余刷新次数：{max(0, min(value, limit))}次"


def settlement_exp_note(applied: Any, requested: Any) -> str:
    """修为被境界上限截断时的说明；未截断返回空串。"""
    applied_value = max(0, _int(applied))
    requested_value = max(0, _int(requested, default=applied_value))
    if applied_value < requested_value:
        return "（已达本境修为上限，超出部分未发放）"
    return ""


__all__ = ["refresh_count_line", "settlement_exp_note"]
