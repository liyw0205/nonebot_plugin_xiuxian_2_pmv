from __future__ import annotations

from pathlib import Path
from threading import RLock
from typing import Any

from ...infrastructure.clock import SystemClock
from .state_repository import TowerStateRepository


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


__all__ = ["TowerStateApplication"]
