from __future__ import annotations

from pathlib import Path

from .._migrated_application import MigratedFeatureApplication
from .repository import AdminRepository


class AdminApplication(MigratedFeatureApplication):
    def __init__(self, database: str | Path, *, repository: AdminRepository | None = None) -> None:
        super().__init__(database, feature="admin", repository=repository or AdminRepository(database))


__all__ = ["AdminApplication"]
