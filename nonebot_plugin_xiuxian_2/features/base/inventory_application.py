from __future__ import annotations

from pathlib import Path

from .inventory_repository import InventoryGrantResult, PlayerInventorySqlRepository


class PlayerInventoryApplication:
    """Application boundary for bounded player item grants."""

    def __init__(
        self,
        database: str | Path,
        *,
        repository: PlayerInventorySqlRepository | None = None,
    ) -> None:
        self.repository = repository or PlayerInventorySqlRepository(database)

    def grant_item(
        self,
        user_id: str,
        item_id: int,
        item_name: str,
        item_type: str,
        quantity: int,
        *,
        bind_flag: int = 0,
        max_goods_num: int,
    ) -> InventoryGrantResult:
        return self.repository.grant(
            user_id,
            item_id,
            item_name,
            item_type,
            quantity,
            bind_flag=bind_flag,
            max_goods_num=max_goods_num,
        )


__all__ = ["InventoryGrantResult", "PlayerInventoryApplication"]
