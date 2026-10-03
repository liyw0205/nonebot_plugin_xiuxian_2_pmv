from __future__ import annotations

from pathlib import Path
from typing import Any

from ...paths import get_paths
from .._migrated_application import MigratedFeatureApplication
from .player_status_batch_repository import AdminPlayerStatusBatchResetSqlRepository
from .player_status_reset_repository import (
    AdminPlayerStatusResetResult,
    AdminPlayerStatusResetSqlRepository,
)
from .repository import AdminRepository


class AdminApplication(MigratedFeatureApplication):
    def __init__(
        self,
        database: str | Path,
        *,
        repository: AdminRepository | None = None,
        player_status_reset_repository: AdminPlayerStatusResetSqlRepository | None = None,
        player_status_batch_repository: AdminPlayerStatusBatchResetSqlRepository | None = None,
    ) -> None:
        super().__init__(database, feature="admin", repository=repository or AdminRepository(database))
        self.player_status_reset_repository = (
            player_status_reset_repository
            or AdminPlayerStatusResetSqlRepository(database)
        )
        self.player_status_batch_repository = (
            player_status_batch_repository
            or AdminPlayerStatusBatchResetSqlRepository(database)
        )

    def player_status_snapshot(
        self, user_id: str
    ) -> tuple[int, int, int, int, int] | None:
        return self.player_status_reset_repository.snapshot(user_id)

    def reset_player_status(self, *args: Any, **kwargs: Any) -> AdminPlayerStatusResetResult:
        return self.player_status_reset_repository.reset(*args, **kwargs)

    def find_player_status_batch(self, operator_id: str, max_stamina: int) -> str | None:
        return self.player_status_batch_repository.find_running(operator_id, max_stamina)

    def reset_player_status_batch(
        self,
        operation_id: str,
        operator_id: str,
        max_stamina: int,
        *,
        chunk_size: int = 100,
    ):
        return self.player_status_batch_repository.reset(
            operation_id,
            operator_id,
            max_stamina,
            chunk_size=chunk_size,
        )

    def grant_accessory_batch(self, operation_id: str, operator_id: str, user_ids,
                              item_id: int, item_name: str, quality: int,
                              quantity: int, max_accessories: int, create_accessory,
                              *, chunk_size: int = 100):
        from ...xiuxian.xiuxian_admin.transaction_service import AdminAccessoryBatchAdjustmentService
        return AdminAccessoryBatchAdjustmentService(
            self.database, get_paths().player_db
        ).grant(
            operation_id, operator_id, user_ids, item_id, item_name, quality,
            quantity, max_accessories, create_accessory, chunk_size=chunk_size,
        )
    def destroy_accessory_batch(self, operation_id: str, operator_id: str, user_ids,
                                item_id: int, item_name: str, quantity: int,
                                *, chunk_size: int = 100):
        from ...xiuxian.xiuxian_admin.transaction_service import AdminAccessoryBatchAdjustmentService
        return AdminAccessoryBatchAdjustmentService(
            self.database, get_paths().player_db
        ).destroy(
            operation_id, operator_id, user_ids, item_id, item_name, quantity,
            chunk_size=chunk_size,
        )
    def set_blackhouse_status(self, *args, **kwargs):
        from ...xiuxian.xiuxian_admin.transaction_service import AdminBlackhouseStatusService
        return AdminBlackhouseStatusService(self.database).set_banned(*args, **kwargs)


__all__ = ["AdminApplication"]
