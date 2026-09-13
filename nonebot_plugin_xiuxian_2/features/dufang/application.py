from __future__ import annotations

from pathlib import Path

from .._migrated_application import MigratedFeatureApplication
from .repository import DufangRepository


class DufangApplication(MigratedFeatureApplication):
    def __init__(self, database: str | Path, *, repository: DufangRepository | None = None) -> None:
        super().__init__(database, feature="dufang", repository=repository or DufangRepository(database))


__all__ = ["DufangApplication"]
