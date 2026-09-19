from __future__ import annotations

from pathlib import Path
from typing import Any

from ...infrastructure.clock import SystemClock
from .maintenance_repository import WorkDailyRefreshResetRepository, WorkDailyRefreshResetResult


class WorkDailyRefreshResetApplication:
    """Feature-owned daily work refresh maintenance boundary."""

    def __init__(
        self,
        database: str | Path,
        *,
        repository: WorkDailyRefreshResetRepository | None = None,
        clock: Any | None = None,
    ) -> None:
        self.repository = repository or WorkDailyRefreshResetRepository(database)
        self.clock = clock or SystemClock()

    def reset(self, business_date: Any, reset_count: int, *, chunk_size: int = 500, updated_at: str | None = None) -> WorkDailyRefreshResetResult:
        stamp = updated_at or self.clock.now().strftime("%Y-%m-%d %H:%M:%S")
        return self.repository.reset(business_date, reset_count, chunk_size=chunk_size, updated_at=stamp)


__all__ = ["WorkDailyRefreshResetApplication"]
