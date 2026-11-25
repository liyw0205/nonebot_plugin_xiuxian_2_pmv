from __future__ import annotations

from pathlib import Path
from threading import RLock
from typing import Any

from ...infrastructure.clock import SystemClock
from .state_repository import TowerFloorResetResult, TowerStateRepository


class TowerStateApplication:
    """Feature-owned tower state initialization boundary."""

    def __init__(
        self,
        player_database: str | Path,
        *,
        repository: TowerStateRepository | None = None,
        clock: Any | None = None,
        lock: Any | None = None,
    ) -> None:
        self.repository = repository or TowerStateRepository(player_database)
        self.clock = clock or SystemClock()
        self.lock = lock or RLock()

    def get(self, user_id: str) -> dict[str, Any]:
        with self.lock:
            return self.repository.initialize(user_id, self.clock.now().date())

    def read(self, user_id: str) -> dict[str, Any]:
        with self.lock:
            return self.repository.read(user_id, self.clock.now().date())

    def reset_all_floors(
        self, *, operation_id: str, source: str, period_key: str
    ) -> TowerFloorResetResult:
        with self.lock:
            return self.repository.reset_all_floors(
                operation_id=operation_id, source=source, period_key=period_key
            )

    def ranking(self, field: str, limit: int = 50) -> list[tuple[str, int]]:
        with self.lock:
            return self.repository.ranking(field, limit)

    def ranking_snapshot(self, field: str, limit: int = 50) -> dict[str, Any]:
        with self.lock:
            return self.repository.ranking_snapshot(field, limit)

    def ranking_snapshot_with_rank(
        self, user_id: str, field: str = "current_floor", limit: int = 50
    ):
        with self.lock:
            return self.repository.ranking_snapshot_with_rank(user_id, field, limit)

    def rank_of(self, user_id: str, field: str = "current_floor"):
        with self.lock:
            return self.repository.rank_of(user_id, field)


__all__ = ["TowerStateApplication"]
