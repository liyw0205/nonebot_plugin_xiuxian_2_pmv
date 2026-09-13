from __future__ import annotations

from pathlib import Path

from .._migrated_application import MigratedFeatureApplication
from .repository import CompensationRepository


class CompensationApplication(MigratedFeatureApplication):
    def __init__(self, database: str | Path, *, repository: CompensationRepository | None = None) -> None:
        super().__init__(database, feature="compensation", repository=repository or CompensationRepository(database))


__all__ = ["CompensationApplication"]
