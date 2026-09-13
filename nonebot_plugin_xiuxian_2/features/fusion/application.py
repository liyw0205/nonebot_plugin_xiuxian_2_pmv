from __future__ import annotations

from pathlib import Path

from .._migrated_application import MigratedFeatureApplication
from .repository import FusionRepository


class FusionApplication(MigratedFeatureApplication):
    def __init__(self, database: str | Path, *, repository: FusionRepository | None = None) -> None:
        super().__init__(database, feature="fusion", repository=repository or FusionRepository(database))


__all__ = ["FusionApplication"]
