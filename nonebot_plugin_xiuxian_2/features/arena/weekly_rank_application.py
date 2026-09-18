from __future__ import annotations

from pathlib import Path
from typing import Any

from .weekly_rank_repository import ArenaWeeklyRankRepository


class ArenaWeeklyRankApplication:
    """Application boundary for resumable weekly arena maintenance."""

    def __init__(
        self,
        player_database: str | Path,
        *,
        repository: ArenaWeeklyRankRepository | None = None,
        clock: Any | None = None,
    ) -> None:
        self.repository = repository or ArenaWeeklyRankRepository(player_database, clock=clock)

    def reduce(self, business_week: Any = None, reduce_steps: int = 2, *, chunk_size: int = 500, updated_at: Any = None):
        if int(reduce_steps) < 0:
            raise ValueError("reduce_steps must be non-negative")
        if int(chunk_size) <= 0:
            raise ValueError("chunk_size must be positive")
        return self.repository.reduce(
            business_week,
            int(reduce_steps),
            chunk_size=int(chunk_size),
            updated_at=updated_at,
        )


__all__ = ["ArenaWeeklyRankApplication"]
