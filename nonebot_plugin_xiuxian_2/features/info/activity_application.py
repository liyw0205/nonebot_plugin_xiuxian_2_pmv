"""Application boundary for player information-view activity."""

from __future__ import annotations

from datetime import datetime
from pathlib import Path

from ...infrastructure.clock import SystemClock
from .activity_repository import PlayerActivitySqlRepository


class PlayerActivityApplication:
    def __init__(
        self,
        database: str | Path,
        *,
        repository: PlayerActivitySqlRepository | None = None,
        clock=None,
    ) -> None:
        self.repository = repository or PlayerActivitySqlRepository(database)
        self.clock = clock or SystemClock()

    def update_last_check_info_time(self, user_id: int | str) -> int:
        occurred_at = self.clock.now()
        if occurred_at.tzinfo is not None:
            # Legacy readers expect a local, timezone-naive timestamp.
            occurred_at = occurred_at.astimezone().replace(tzinfo=None)
        return self.repository.update_last_check_info_time(
            str(user_id), occurred_at.isoformat(sep=" ")
        )

    def get_last_check_info_time(self, user_id: int | str) -> datetime | None:
        return self.repository.get_last_check_info_time(str(user_id))


__all__ = ["PlayerActivityApplication"]
