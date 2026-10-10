from __future__ import annotations

from pathlib import Path

from .schemas import GameEventStatisticsRequest, GameEventStatisticsResult
from .statistics import GameEventStatisticsRepository


class GameEventApplication:
    """Application boundary for durable player statistics projections."""

    def __init__(
        self,
        player_database: str | Path,
        *,
        repository: GameEventStatisticsRepository | None = None,
    ) -> None:
        self.repository = repository or GameEventStatisticsRepository(player_database)

    def record_statistics(
        self,
        request: GameEventStatisticsRequest,
    ) -> GameEventStatisticsResult:
        request.validate()
        changed = self.repository.record(
            event_id=request.event_id.strip(),
            user_id=request.user_id.strip(),
            increments={str(key).strip(): int(value) for key, value in request.increments.items()},
            occurred_at=request.occurred_at.strip(),
        )
        return GameEventStatisticsResult(
            event_id=request.event_id.strip(),
            user_id=request.user_id.strip(),
            status="applied" if changed else "replayed",
            changed=changed,
        )


__all__ = ["GameEventApplication"]
