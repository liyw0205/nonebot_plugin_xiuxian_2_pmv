from __future__ import annotations

from pathlib import Path

from .item_use_repository import ItemUseResult, ItemUseSqlRepository


class ItemUseApplication:
    """Application boundary for the generic Web backpack batch action."""

    def __init__(
        self,
        database: str | Path,
        *,
        repository: ItemUseSqlRepository | None = None,
    ) -> None:
        self.repository = repository or ItemUseSqlRepository(database)

    def apply(
        self,
        operation_id: str,
        user_id: str,
        item_id: int,
        quantity: int,
        expected_item_count: int | None = None,
    ) -> ItemUseResult:
        return self.repository.apply(
            operation_id,
            user_id,
            item_id,
            quantity,
            expected_item_count,
        )


__all__ = ["ItemUseApplication", "ItemUseResult"]
