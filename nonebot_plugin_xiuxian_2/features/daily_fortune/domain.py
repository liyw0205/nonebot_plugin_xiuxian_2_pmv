from __future__ import annotations

from dataclasses import dataclass
from typing import Protocol


class RandomLike(Protocol):
    def randint(self, start: int, end: int) -> int: ...


@dataclass(frozen=True)
class FortuneValue:
    score: int
    title: str
    message: str


_MESSAGES = (
    ("大吉", "今日气运通达，适合推进重要计划。"),
    ("吉", "今日稳中有进，静心即可有所收获。"),
    ("平", "今日宜守不宜躁，积累亦是修行。"),
    ("小凶", "今日变数较多，重要决定请多核验一次。"),
)


def draw_fortune(random_source: RandomLike) -> FortuneValue:
    score = int(random_source.randint(1, 100))
    if score >= 85:
        index = 0
    elif score >= 60:
        index = 1
    elif score >= 30:
        index = 2
    else:
        index = 3
    title, message = _MESSAGES[index]
    return FortuneValue(score, title, message)


__all__ = ["FortuneValue", "draw_fortune"]
