from __future__ import annotations

from pathlib import Path

from .._migrated_application import MigratedFeatureApplication
from .repository import DufangRepository


class DufangApplication(MigratedFeatureApplication):
    def __init__(
        self,
        database: str | Path,
        player_database: str | Path | None = None,
        *,
        repository: DufangRepository | None = None,
    ) -> None:
        super().__init__(
            database,
            feature="dufang",
            repository=repository or DufangRepository(database, player_database),
        )

    def bet(self, *, operation_id: str, user_id: str, **kwargs):
        return self.execute(
            operation_id=operation_id,
            user_id=user_id,
            payload={"action": "bet", **kwargs},
            ledger_payload={"cost": kwargs["cost"]},
        )


__all__ = ["DufangApplication"]
