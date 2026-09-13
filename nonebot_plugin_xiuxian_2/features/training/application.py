from __future__ import annotations

from pathlib import Path

from .._migrated_application import MigratedFeatureApplication
from .repository import TrainingRepository


class TrainingApplication(MigratedFeatureApplication):
    def __init__(self, database: str | Path, *, repository: TrainingRepository | None = None) -> None:
        super().__init__(database, feature="training", repository=repository or TrainingRepository(database))


__all__ = ["TrainingApplication"]
