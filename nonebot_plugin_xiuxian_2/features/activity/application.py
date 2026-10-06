from __future__ import annotations

from pathlib import Path

from .._migrated_application import MigratedFeatureApplication
from .config_application import ActivityConfigApplication
from .read_model_application import ActivityReadModelApplication
from .repository import ActivityRepository


class ActivityApplication(MigratedFeatureApplication):
    def __init__(self, database: str | Path, *, repository: ActivityRepository | None = None) -> None:
        super().__init__(database, feature="activity", repository=repository or ActivityRepository(database))
        self.read_model = ActivityReadModelApplication(database)
        self.config = ActivityConfigApplication()


__all__ = ["ActivityApplication"]
