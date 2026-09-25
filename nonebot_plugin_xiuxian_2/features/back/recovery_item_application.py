from __future__ import annotations

from pathlib import Path

from .recovery_item_repository import RecoveryItemResult, RecoveryItemSqlRepository


class RecoveryItemApplication:
    """Feature-owned use case for recovery elixirs."""

    def __init__(
        self,
        database: str | Path,
        *,
        repository: RecoveryItemSqlRepository | None = None,
    ) -> None:
        self.repository = repository or RecoveryItemSqlRepository(database)

    def apply(self, operation_id: str, user_id: str, *args, **kwargs) -> RecoveryItemResult:
        return self.repository.apply(operation_id, user_id, *args, **kwargs)


__all__ = ["RecoveryItemApplication", "RecoveryItemResult"]
