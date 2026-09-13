"""System-backed implementation of the application random source port."""

from __future__ import annotations

import random


class SystemRandom:
    """Provide cryptographically strong random draws behind a small port."""

    def __init__(self) -> None:
        self._source = random.SystemRandom()

    def random(self) -> float:
        return self._source.random()

    def randint(self, start: int, end: int) -> int:
        return self._source.randint(start, end)


__all__ = ["SystemRandom"]
