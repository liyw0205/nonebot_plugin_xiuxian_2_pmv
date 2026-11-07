"""商店「每周限购」行的共享格式化（纯函数，无 I/O、无 NoneBot）。

四个商店（宗门、世界BOSS、通天塔、历练、竞技场）此前各自拼限购文案：
- 宗门商店只写总量 `每周限购：{limit}个`，与其它三家 `已购/总量` 的形状不一致，
  玩家看不到本周还剩多少；
- 其余店铺把「剩余/总量」写死在命令层，同一规则四处复制。

这里把口径收到一处：
- `remaining = max(limit - purchased, 0)`；
- `limit > 0 且 remaining == 0` → `每周限购：已售罄（{limit}/{limit}个）`；
- `limit > 0` → `每周限购：{remaining}/{limit}个`；
- `limit <= 0`（配置未设置或显式不限购）→ `每周限购：不限购`。

调用方只提供 limit 与已购数量，不再自行算差值。
"""

from __future__ import annotations

from typing import Any

__all__ = ["shop_limit_line", "shop_limit_parts"]


def _int(value: Any, default: int = 0) -> int:
    try:
        return int(value)
    except (TypeError, ValueError, OverflowError):
        return default


def shop_limit_parts(limit: Any, purchased: Any = 0) -> tuple[int, int]:
    """返回 `(remaining, limit)`；非法/负限量按不限购（`limit <= 0`）处理。"""
    resolved = _int(limit)
    if resolved <= 0:
        return 0, resolved
    remaining = resolved - max(0, _int(purchased))
    return max(remaining, 0), resolved


def shop_limit_line(limit: Any, purchased: Any = 0, *, prefix: str = "每周限购：") -> str:
    """渲染统一的每周限购行（宗门/世界BOSS/通天塔/历练/竞技场共用）。"""
    remaining, resolved = shop_limit_parts(limit, purchased)
    if resolved <= 0:
        return f"{prefix}不限购"
    if remaining <= 0:
        return f"{prefix}已售罄（{resolved}/{resolved}个）"
    return f"{prefix}{remaining}/{resolved}个"
