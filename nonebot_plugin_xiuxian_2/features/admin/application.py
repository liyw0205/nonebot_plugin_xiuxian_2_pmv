from __future__ import annotations

from pathlib import Path

from .._migrated_application import MigratedFeatureApplication
from .repository import AdminRepository


class AdminApplication(MigratedFeatureApplication):
    def __init__(self, database: str | Path, *, repository: AdminRepository | None = None) -> None:
        super().__init__(database, feature="admin", repository=repository or AdminRepository(database))

    def find_player_status_batch(self, operator_id: str, max_stamina: int) -> str | None:
        from ...xiuxian.xiuxian_admin.transaction_service import AdminPlayerStatusBatchResetService
        return AdminPlayerStatusBatchResetService(self.database).find_running(operator_id, max_stamina)

    def reset_player_status_batch(self, *args, **kwargs):
        from ...xiuxian.xiuxian_admin.transaction_service import AdminPlayerStatusBatchResetService
        return AdminPlayerStatusBatchResetService(self.database).reset(*args, **kwargs)

    def grant_item_batch(self, *args, **kwargs):
        from ...xiuxian.xiuxian_admin.transaction_service import AdminItemBatchGrantService
        return AdminItemBatchGrantService(self.database).grant(*args, **kwargs)


__all__ = ["AdminApplication"]
