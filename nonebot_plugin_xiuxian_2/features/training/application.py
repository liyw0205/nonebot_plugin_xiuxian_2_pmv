from __future__ import annotations

from pathlib import Path

from .._migrated_application import MigratedFeatureApplication
from ...infrastructure.database import DatabaseUnitOfWork
from .repository import TrainingRepository


class TrainingApplication(MigratedFeatureApplication):
    def __init__(
        self,
        database: str | Path,
        player_database: str | Path | None = None,
        *,
        repository: TrainingRepository | None = None,
    ) -> None:
        super().__init__(
            database,
            feature="training",
            repository=repository or TrainingRepository(database, player_database),
        )
        # Keep the standalone compatibility application usable in maintenance
        # contexts where the full composition-root migration has not run yet.
        with DatabaseUnitOfWork(self.database) as uow:
            self.ledger.ensure_schema(uow)


__all__ = ["TrainingApplication"]
