"""Explicit rollback adapter for the historical auction queue service."""

from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from threading import RLock


@dataclass(frozen=True)
class AuctionQueueResult:
    status: str
    action: str
    user_id: str
    item_id: int
    item_name: str = ""
    start_price: int = 0
    user_name: str = ""

    @property
    def succeeded(self) -> bool:
        return self.status in {"completed", "duplicate"}

    @property
    def applied(self) -> bool:
        return self.status == "completed"


class AuctionQueueService:
    """Compatibility facade backed by the feature-owned queue repository."""

    def __init__(
        self,
        game_database: str | Path,
        trade_database: str | Path,
        max_goods_num: int,
        lock: RLock | None = None,
    ) -> None:
        self._game_database = Path(game_database)
        self._trade_database = Path(trade_database)
        self._max_goods_num = max(int(max_goods_num), 1)
        from ..features.auction.queue_repository import AuctionQueueSqlRepository

        self._repository = AuctionQueueSqlRepository(
            self._game_database, self._trade_database, self._max_goods_num
        )

    @staticmethod
    def _compat_result(result):
        if result is None:
            return None
        return AuctionQueueResult(
            result.status,
            result.action,
            result.user_id,
            result.item_id,
            result.item_name,
            result.start_price,
            result.user_name,
        )

    def get_operation(self, operation_id, action, user_id, item_id):
        return self._compat_result(
            self._repository.get_operation(operation_id, action, user_id, item_id)
        )

    def enqueue(
        self,
        operation_id,
        user_id,
        item_id,
        item_name,
        start_price,
        user_name,
        *,
        max_user_items: int,
    ) -> AuctionQueueResult:
        return self._compat_result(
            self._repository.enqueue(
                operation_id,
                user_id,
                item_id,
                item_name,
                start_price,
                user_name,
                max_user_items=max_user_items,
            )
        )

    def dequeue(self, operation_id, user_id, item_id, item_type) -> AuctionQueueResult:
        return self._compat_result(
            self._repository.dequeue(operation_id, user_id, item_id, item_type)
        )


__all__ = ["AuctionQueueResult", "AuctionQueueService"]
