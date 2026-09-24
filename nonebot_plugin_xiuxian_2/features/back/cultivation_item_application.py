from __future__ import annotations

from pathlib import Path

from .cultivation_item_repository import CultivationItemResult, CultivationItemSqlRepository


class CultivationItemApplication:
    """Feature-owned use case for consuming cultivation items."""

    def __init__(self, database: str | Path, *, repository: CultivationItemSqlRepository | None = None) -> None:
        self.repository = repository or CultivationItemSqlRepository(database)

    def apply(self, operation_id: str, user_id: str, item_id: int, quantity: int, exp_gain: int, **kwargs) -> CultivationItemResult:
        return self.repository.apply(operation_id, user_id, item_id, quantity, exp_gain, **kwargs)


__all__ = ["CultivationItemApplication", "CultivationItemResult"]
