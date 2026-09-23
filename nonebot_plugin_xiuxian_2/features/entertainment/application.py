from __future__ import annotations

from pathlib import Path

from .._migrated_application import MigratedFeatureApplication
from .repository import EntertainmentRepository


class EntertainmentApplication(MigratedFeatureApplication):
    def __init__(self, database: str | Path, *, repository: EntertainmentRepository | None = None) -> None:
        super().__init__(database, feature="entertainment", repository=repository or EntertainmentRepository(database))

    def toggle_auto_checkin(self, *, operation_id: str, user_id: str, state_path: str | Path, index: int):
        return self.execute(operation_id=operation_id, user_id=user_id, payload={"action": "toggle_auto_checkin", "state_path": str(state_path), "index": int(index)})


__all__ = ["EntertainmentApplication"]
