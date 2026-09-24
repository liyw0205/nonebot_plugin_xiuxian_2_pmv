from __future__ import annotations

from pathlib import Path

from .unbind_repository import UnbindResult, UnbindSqlRepository


class UnbindApplication:
    """Feature-owned use case for consuming an unbind charm."""

    def __init__(self, database: str | Path, *, repository: UnbindSqlRepository | None = None) -> None:
        self.repository = repository or UnbindSqlRepository(database)

    def apply(
        self,
        operation_id: str,
        user_id: str,
        charm_item_id: int,
        target_item_id: int,
        requested_quantity: int,
    ) -> UnbindResult:
        return self.repository.apply(
            operation_id,
            user_id,
            charm_item_id,
            target_item_id,
            requested_quantity,
        )


__all__ = ["UnbindApplication", "UnbindResult"]
