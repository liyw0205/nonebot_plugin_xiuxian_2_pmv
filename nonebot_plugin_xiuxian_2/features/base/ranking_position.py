"""排行榜的“本人名次”共享模型与渲染（纯函数，无 I/O、无 NoneBot）。

六个排行榜（世界BOSS积分、通天塔层数/积分、历练完成/积分、竞技场积分）此前只渲染
前 N 名；榜外玩家既看不到自己的位置，也看不到与榜尾的差距。这里统一口径：

- `rank` 为**并列名次**（competition rank）：`1 + 严格高于本人的人数`，同分同名次；
- `total` 为有有效记录（数值 > 0）的去重人数；
- `rank <= 0` 表示本人没有有效记录，渲染为“还没有…记录”，不伪造名次。

仓储只负责用 SQL 取这三个数（缺库/缺表返回空 `PersonalRank`），渲染集中在
`personal_rank_line`，命令层只拼接。
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Callable


@dataclass(frozen=True)
class PersonalRank:
    rank: int = 0
    total: int = 0
    value: int = 0

    @property
    def ranked(self) -> bool:
        return int(self.rank) > 0


def personal_rank_line(
    entry: PersonalRank | None,
    *,
    label: str,
    unit: str = "",
    number: Callable[[int], str] | None = None,
) -> str:
    """渲染本人名次行；无有效记录时给出明确空态。"""
    number = number or (lambda value: str(value))
    if entry is None or int(entry.rank) <= 0:
        return f"你还没有{label}记录。"
    rank = int(entry.rank)
    total = max(rank, int(entry.total))
    return f"你的名次：第{rank}名 / 共{total}人（{label} {number(int(entry.value))}{unit}）"


__all__ = ["PersonalRank", "personal_rank_line"]
