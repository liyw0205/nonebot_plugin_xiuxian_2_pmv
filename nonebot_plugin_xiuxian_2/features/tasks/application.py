from __future__ import annotations

from collections.abc import Iterable, Mapping
from pathlib import Path
from typing import Any

from .._migrated_application import MigratedFeatureApplication
from .repository import TasksRepository
from .progress import TaskProgressEventResult, TasksProgressRepository


class TasksApplication(MigratedFeatureApplication):
    def __init__(self, database: str | Path, *, repository: TasksRepository | None = None) -> None:
        super().__init__(database, feature="tasks", repository=repository or TasksRepository(database))


class TaskProgressApplication:
    def __init__(
        self,
        player_database: str | Path,
        *,
        repository: TasksProgressRepository | None = None,
    ) -> None:
        self.repository = repository or TasksProgressRepository(player_database)

    def record(
        self,
        operation_id: str,
        user_id: str,
        events: Iterable[tuple[str, int]],
        periods: Mapping[str, str],
        tasks: Iterable[Mapping[str, Any]],
    ) -> TaskProgressEventResult:
        return self.repository.record(operation_id, user_id, events, periods, tasks)

    def get_states(
        self, user_id: str, periods: Mapping[str, str]
    ) -> dict[str, tuple[dict[str, int], list[str], str]]:
        return self.repository.get_states(user_id, periods)


__all__ = ["TasksApplication", "TaskProgressApplication"]
