from __future__ import annotations

from collections.abc import Sequence

from .season_reward_repository import ArenaHonorSettlement


def render_honor_settlements(
    settlements: Sequence[ArenaHonorSettlement], *, limit: int = 5
) -> str:
    size = max(int(limit), 0)
    rows = list(settlements or ())[:size]
    lines = ["", "【最近荣誉结算】"]
    if not rows:
        lines.append("暂无荣誉结算记录；每日 0 点结算后在此显示。")
        return "\n".join(lines)
    for item in rows:
        rank = ""
        if item.rank and item.position:
            rank = f"{item.rank}第{item.position}名"
        elif item.rank:
            rank = item.rank
        if item.details_available:
            suffix = f"（{rank}，结算后荣誉 {item.honor_after}）" if rank else f"（结算后荣誉 {item.honor_after}）"
            lines.append(
                f"{item.season_key}：基础 {item.base_honor} + 排名 {item.ranking_bonus} "
                f"= 本次 {item.total_honor}{suffix}"
            )
        else:
            lines.append(
                f"{item.season_key}：本次 +{item.total_honor} 荣誉"
                f"（结算后 {item.honor_after}；明细不可用）"
            )
    lines.append(f"每日 0 点自动结算；最多显示最近 {max(size, 1)} 次。")
    return "\n".join(lines)


__all__ = ["render_honor_settlements"]
