from __future__ import annotations

from collections import Counter
from threading import Lock


class Metrics:
    def __init__(self) -> None:
        self._values: Counter[str] = Counter()
        self._lock = Lock()

    def increment(self, name: str, amount: int = 1) -> int:
        if not name:
            raise ValueError("metric name is required")
        with self._lock:
            self._values[name] += amount
            return self._values[name]

    def set(self, name: str, value: int) -> None:
        with self._lock:
            self._values[name] = int(value)

    def get(self, name: str) -> int:
        with self._lock:
            return self._values.get(name, 0)

    def snapshot(self) -> dict[str, int]:
        with self._lock:
            return dict(self._values)


__all__ = ["Metrics"]
