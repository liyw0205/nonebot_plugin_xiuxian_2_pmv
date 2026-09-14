"""System-backed implementation of the application random source port."""

from __future__ import annotations

import random
from typing import Sequence, TypeVar


T = TypeVar("T")


class SystemRandom:
    """Provide cryptographically strong random draws behind a small port."""

    def __init__(self) -> None:
        self._source = random.SystemRandom()

    def random(self) -> float:
        return self._source.random()

    def randint(self, start: int, end: int) -> int:
        return self._source.randint(start, end)

    def uniform(self, start: float, end: float) -> float:
        return self._source.uniform(start, end)

    def choice(self, values: Sequence[T]) -> T:
        return self._source.choice(values)


__all__ = ["SystemRandom"]
