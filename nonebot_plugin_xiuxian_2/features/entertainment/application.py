from __future__ import annotations

from pathlib import Path

from .._migrated_application import MigratedFeatureApplication
from .repository import EntertainmentRepository


class EntertainmentApplication(MigratedFeatureApplication):
    def __init__(self, database: str | Path, *, repository: EntertainmentRepository | None = None) -> None:
        super().__init__(database, feature="entertainment", repository=repository or EntertainmentRepository(database))


__all__ = ["EntertainmentApplication"]
