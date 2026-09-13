from __future__ import annotations

from pathlib import Path

from .._migrated_application import MigratedFeatureApplication
from .repository import SimulatorRepository


class SimulatorApplication(MigratedFeatureApplication):
    def __init__(self, database: str | Path, *, repository: SimulatorRepository | None = None) -> None:
        super().__init__(database, feature="simulator", repository=repository or SimulatorRepository(database))


__all__ = ["SimulatorApplication"]
