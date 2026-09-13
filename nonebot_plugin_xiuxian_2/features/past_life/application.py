from __future__ import annotations

from pathlib import Path

from .._migrated_application import MigratedFeatureApplication
from .repository import PastLifeRepository


class PastLifeApplication(MigratedFeatureApplication):
    def __init__(self, database: str | Path, *, repository: PastLifeRepository | None = None) -> None:
        super().__init__(database, feature="past_life", repository=repository or PastLifeRepository(database))


__all__ = ["PastLifeApplication"]
