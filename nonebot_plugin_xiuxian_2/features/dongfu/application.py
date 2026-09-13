from __future__ import annotations

from pathlib import Path

from .._migrated_application import MigratedFeatureApplication
from .repository import DongfuRepository


class DongfuApplication(MigratedFeatureApplication):
    def __init__(self, database: str | Path, *, repository: DongfuRepository | None = None) -> None:
        super().__init__(database, feature="dongfu", repository=repository or DongfuRepository(database))


__all__ = ["DongfuApplication"]
