from __future__ import annotations

from pathlib import Path

from .._migrated_application import MigratedFeatureApplication
from .repository import ImpartRepository


class ImpartApplication(MigratedFeatureApplication):
    def __init__(self, database: str | Path, *, repository: ImpartRepository | None = None) -> None:
        super().__init__(database, feature="impart", repository=repository or ImpartRepository(database))


__all__ = ["ImpartApplication"]
