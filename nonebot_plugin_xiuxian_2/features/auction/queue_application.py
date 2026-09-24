from __future__ import annotations

from pathlib import Path
from typing import Any

from .queue_repository import AuctionQueueSqlRepository


class AuctionQueueApplication:
    def __init__(
        self,
        game_database: str | Path,
        trade_database: str | Path,
        max_goods_num: int,
        *,
        repository: AuctionQueueSqlRepository | None = None,
        clock: Any | None = None,
    ) -> None:
        self.repository = repository or AuctionQueueSqlRepository(
            game_database, trade_database, max_goods_num, clock=clock
        )

    def get_operation(self, operation_id: str, action: str, user_id: str, item_id: int):
        return self.repository.get_operation(operation_id, action, user_id, item_id)

    def enqueue(
        self,
        operation_id: str,
        user_id: str,
        item_id: int,
        item_name: str,
        start_price: int,
        user_name: str,
        *,
        max_user_items: int,
    ):
        return self.repository.enqueue(
            operation_id,
            user_id,
            item_id,
            item_name,
            start_price,
            user_name,
            max_user_items=max_user_items,
        )

    def dequeue(
        self, operation_id: str, user_id: str, item_id: int, item_type: str
    ):
        return self.repository.dequeue(operation_id, user_id, item_id, item_type)


__all__ = ["AuctionQueueApplication"]
