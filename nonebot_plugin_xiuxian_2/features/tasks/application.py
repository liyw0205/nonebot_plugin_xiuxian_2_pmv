from __future__ import annotations

from pathlib import Path

from .._migrated_application import MigratedFeatureApplication
from .repository import TasksRepository


class TasksApplication(MigratedFeatureApplication):
    def __init__(self, database: str | Path, *, repository: TasksRepository | None = None) -> None:
        super().__init__(database, feature="tasks", repository=repository or TasksRepository(database))


__all__ = ["TasksApplication"]
