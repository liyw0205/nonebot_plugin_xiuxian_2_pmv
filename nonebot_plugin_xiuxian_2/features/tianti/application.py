from __future__ import annotations

from pathlib import Path

from .._migrated_application import MigratedFeatureApplication
from .repository import TiantiRepository


class TiantiApplication(MigratedFeatureApplication):
    def __init__(self, database: str | Path, *, repository: TiantiRepository | None = None) -> None:
        super().__init__(database, feature="tianti", repository=repository or TiantiRepository(database))


__all__ = ["TiantiApplication"]
