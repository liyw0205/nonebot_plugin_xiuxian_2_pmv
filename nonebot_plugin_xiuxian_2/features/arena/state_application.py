from __future__ import annotations

from pathlib import Path
from threading import RLock
from typing import Any

from ...infrastructure.clock import SystemClock
from .state_repository import ArenaStateRepository


class ArenaStateApplication:
    """Feature-owned arena state initialization boundary."""

    def __init__(
        self,
        player_database: str | Path,
        *,
        repository: ArenaStateRepository | None = None,
        clock: Any | None = None,
        lock: Any | None = None,
    ) -> None:
        self.repository = repository or ArenaStateRepository(player_database)
        self.clock = clock or SystemClock()
        self.lock = lock or RLock()

    def get(self, user_id: str) -> dict[str, Any]:
        with self.lock:
            return self.repository.initialize(user_id, self.clock.now().date())

    def ranking(self, limit: int) -> tuple[tuple[str, int], ...]:
        with self.lock:
            return self.repository.ranking(limit)


__all__ = ["ArenaStateApplication"]
