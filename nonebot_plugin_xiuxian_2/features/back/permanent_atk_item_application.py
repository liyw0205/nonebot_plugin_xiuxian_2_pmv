from __future__ import annotations

from pathlib import Path

from .permanent_atk_item_repository import (
    PermanentAtkItemResult,
    PermanentAtkItemSqlRepository,
)


class PermanentAtkItemApplication:
    """Feature-owned use case for permanent attack elixirs."""

    def __init__(
        self,
        database: str | Path,
        *,
        repository: PermanentAtkItemSqlRepository | None = None,
    ) -> None:
        self.repository = repository or PermanentAtkItemSqlRepository(database)

    def apply(self, operation_id: str, user_id: str, *args, **kwargs) -> PermanentAtkItemResult:
        return self.repository.apply(operation_id, user_id, *args, **kwargs)


__all__ = ["PermanentAtkItemApplication", "PermanentAtkItemResult"]
