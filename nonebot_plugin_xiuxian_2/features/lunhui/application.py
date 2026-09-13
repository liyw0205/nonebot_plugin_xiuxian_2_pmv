from __future__ import annotations

from pathlib import Path

from .._migrated_application import MigratedFeatureApplication
from .repository import LunhuiRepository


class LunhuiApplication(MigratedFeatureApplication):
    def __init__(self, database: str | Path, *databases: str | Path, repository: LunhuiRepository | None = None) -> None:
        super().__init__(database, feature="lunhui", repository=repository or LunhuiRepository(database, *databases))


__all__ = ["LunhuiApplication"]
