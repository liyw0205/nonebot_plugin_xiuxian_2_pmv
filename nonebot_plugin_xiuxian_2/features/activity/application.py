from __future__ import annotations

from pathlib import Path
from typing import Any, Mapping

from ...core.result import OperationOutcome
from ...infrastructure.database.uow import DatabaseUnitOfWork, STONE_TRAINING_WRITE_OWNER
from .._migrated_application import MigratedFeatureApplication
from .admin_data_application import ActivityAdminDataApplication
from .config_application import ActivityConfigApplication
from .read_model_application import ActivityReadModelApplication
from .repository import ActivityRepository


class ActivityApplication(MigratedFeatureApplication):
    def __init__(self, database: str | Path, *, repository: ActivityRepository | None = None) -> None:
        super().__init__(database, feature="activity", repository=repository or ActivityRepository(database))
        self.read_model = ActivityReadModelApplication(database)
        self.admin_data = ActivityAdminDataApplication(database)
        self.config = ActivityConfigApplication()

    def execute(
        self,
        *,
        operation_id: str,
        user_id: str,
        payload: Mapping[str, Any] | None = None,
        ledger_payload: Mapping[str, Any] | None = None,
    ) -> OperationOutcome[dict[str, Any]]:
        request = dict(payload or {})
        action = str(request.get("action", request.get("operation", "execute")) or "execute")
        arguments = dict(operation_id=operation_id, user_id=user_id,
                         payload=request, ledger_payload=ledger_payload)
        owner = (
            STONE_TRAINING_WRITE_OWNER.when_unscoped()
            if action in {"activity_boss.fight_cooperative_boss", "activity_boss.use_item_on_boss"}
            else None
        )
        if owner is None:
            return super().execute(**arguments)
        with DatabaseUnitOfWork.connection_scope(
            write_owner=owner, write_database=self.database, reuse_live_readers=True,
        ):
            return super().execute(**arguments)


__all__ = ["ActivityApplication"]
