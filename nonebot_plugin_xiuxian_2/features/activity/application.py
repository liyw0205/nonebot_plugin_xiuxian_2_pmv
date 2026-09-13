from __future__ import annotations

from pathlib import Path

from .._migrated_application import MigratedFeatureApplication
from .repository import ActivityRepository


class ActivityApplication(MigratedFeatureApplication):
    def __init__(self, database: str | Path, *, repository: ActivityRepository | None = None) -> None:
        super().__init__(database, feature="activity", repository=repository or ActivityRepository(database))


__all__ = ["ActivityApplication"]
