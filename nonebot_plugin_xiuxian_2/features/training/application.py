from __future__ import annotations

from pathlib import Path
from typing import Any

from .._migrated_application import MigratedFeatureApplication
from ...infrastructure.database import DatabaseUnitOfWork
from ...infrastructure.clock import SystemClock
from .repository import TrainingRepository


class TrainingApplication(MigratedFeatureApplication):
    def __init__(
        self,
        database: str | Path,
        player_database: str | Path | None = None,
        *,
        repository: TrainingRepository | None = None,
        clock: Any | None = None,
    ) -> None:
        self.clock = clock or SystemClock()
        super().__init__(
            database,
            feature="training",
            repository=repository or TrainingRepository(
                database, player_database, clock=self.clock
            ),
        )
        # Keep the standalone compatibility application usable in maintenance
        # contexts where the full composition-root migration has not run yet.
        with DatabaseUnitOfWork(self.database) as uow:
            self.ledger.ensure_schema(uow)

    def reset_limits(self, *, operation_id: str, operator_id: str, chunk_size: int = 500, **kwargs: Any):
        """Process one administrator reset chunk.

        Reset owns a resumable operation row and is intentionally outside the
        regular per-request operation ledger: the admin worker calls the same
        operation id once per chunk until the target table is complete.
        """
        return self.repository.reset_limits(
            operation_id=operation_id,
            operator_id=operator_id,
            chunk_size=chunk_size,
            **kwargs,
        )


__all__ = ["TrainingApplication"]
