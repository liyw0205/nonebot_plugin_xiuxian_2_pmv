from __future__ import annotations

from datetime import datetime
from typing import Any, Callable

_AUTO_PSUTIL = object()


def _optional_psutil() -> Any | None:
    try:
        import psutil
    except ImportError:
        return None
    return psutil


class ProcessInfoProvider:
    """Collect the legacy process rows through an injectable psutil boundary."""

    def __init__(
        self,
        *,
        psutil_module: Any = _AUTO_PSUTIL,
        now: Callable[[], datetime] = datetime.now,
        from_timestamp: Callable[[float], datetime] = datetime.fromtimestamp,
    ) -> None:
        self._psutil = _optional_psutil() if psutil_module is _AUTO_PSUTIL else psutil_module
        self._now = now
        self._from_timestamp = from_timestamp

    @property
    def available(self) -> bool:
        return self._psutil is not None

    def snapshot(self, limit: int = 5) -> list[dict[str, Any]]:
        if self._psutil is None or limit <= 0:
            return []

        processes = []
        for process in self._psutil.process_iter(["pid", "name", "memory_percent", "create_time"]):
            try:
                memory_mb = process.memory_info().rss / 1024 / 1024
                create_time = self._from_timestamp(process.create_time())
                run_time = self._now() - create_time
                processes.append(
                    {
                        "pid": process.pid,
                        "name": process.name(),
                        "memory": f"{memory_mb:.1f}MB",
                        "memory_mb": round(memory_mb, 1),
                        "time": str(run_time).split(".")[0],
                    }
                )
            except Exception:
                continue

        processes.sort(key=lambda item: item["memory_mb"], reverse=True)
        return processes[:limit]


__all__ = ["ProcessInfoProvider"]
