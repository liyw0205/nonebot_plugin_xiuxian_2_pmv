from __future__ import annotations

from collections import Counter
from threading import Lock


class RuntimeMetrics:
    """进程内轻量计数器，供可靠性路径统一记录与诊断。"""

    def __init__(self) -> None:
        self._values: Counter[str] = Counter()
        self._lock = Lock()

    def increment(self, name: str, amount: int = 1) -> int:
        if not name:
            raise ValueError("指标名称不能为空")
        with self._lock:
            self._values[name] += int(amount)
            return self._values[name]

    def set(self, name: str, value: int) -> None:
        if not name:
            raise ValueError("指标名称不能为空")
        with self._lock:
            self._values[name] = int(value)

    def get(self, name: str) -> int:
        with self._lock:
            return self._values.get(name, 0)

    def snapshot(self, *, prefix: str = "") -> dict[str, int]:
        with self._lock:
            return {
                name: value
                for name, value in self._values.items()
                if not prefix or name.startswith(prefix)
            }

    def clear(self) -> None:
        with self._lock:
            self._values.clear()


runtime_metrics = RuntimeMetrics()


__all__ = ["RuntimeMetrics", "runtime_metrics"]
