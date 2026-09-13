from __future__ import annotations

from pathlib import Path

from .._migrated_application import MigratedFeatureApplication
from .repository import StatusRepository


class StatusApplication(MigratedFeatureApplication):
    def __init__(self, database: str | Path, *, repository: StatusRepository | None = None) -> None:
        super().__init__(database, feature="status", repository=repository or StatusRepository(database))


__all__ = ["StatusApplication"]
