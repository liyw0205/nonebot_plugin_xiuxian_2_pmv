from __future__ import annotations

from pathlib import Path

from .._migrated_application import MigratedFeatureApplication
from .repository import PastLifeRepository


class PastLifeApplication(MigratedFeatureApplication):
    def __init__(self, database: str | Path, player_database: str | Path | None = None, *, repository: PastLifeRepository | None = None) -> None:
        super().__init__(database, feature="past_life", repository=repository or PastLifeRepository(database, player_database))

    def reset_one(self, *, operation_id: str, user_id: str, clear_history: bool):
        return self.execute(
            operation_id=operation_id,
            user_id=user_id,
            payload={"action": "reset_one", "clear_history": bool(clear_history)},
        )


__all__ = ["PastLifeApplication"]
