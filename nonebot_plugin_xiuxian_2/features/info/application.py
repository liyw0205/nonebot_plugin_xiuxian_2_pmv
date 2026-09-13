from __future__ import annotations

from pathlib import Path

from .._migrated_application import MigratedFeatureApplication
from .repository import InfoRepository


class InfoApplication(MigratedFeatureApplication):
    def __init__(self, database: str | Path, *, repository: InfoRepository | None = None) -> None:
        super().__init__(database, feature="info", repository=repository or InfoRepository(database))


__all__ = ["InfoApplication"]
