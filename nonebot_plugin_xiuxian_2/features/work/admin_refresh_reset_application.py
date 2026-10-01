from __future__ import annotations

from pathlib import Path

from ...infrastructure.database import OperationLedger
from .admin_refresh_reset_repository import (
    WorkAdminRefreshResetResult,
    WorkAdminRefreshResetSqlRepository,
)


class WorkAdminRefreshResetApplication:
    def __init__(
        self,
        database: str | Path,
        *,
        repository: WorkAdminRefreshResetSqlRepository | None = None,
        ledger: OperationLedger | None = None,
    ) -> None:
        self.repository = repository or WorkAdminRefreshResetSqlRepository(database, ledger=ledger)

    def reset_all(
        self,
        operation_id: str,
        operator_id: str,
        reset_count: int,
    ) -> WorkAdminRefreshResetResult:
        return self.repository.reset_all(operation_id, operator_id, reset_count)


__all__ = ["WorkAdminRefreshResetApplication"]
