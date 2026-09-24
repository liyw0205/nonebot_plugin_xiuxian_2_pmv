from __future__ import annotations

from pathlib import Path

from ...core.errors import OperationConflictError
from ...infrastructure.database import DatabaseUnitOfWork


class AuctionBidStatisticsRepository:
    """Idempotently project bid counters into the legacy player statistics table."""

    _METRICS = ("拍卖出价次数", "拍卖出价灵石")

    def __init__(self, database: str | Path) -> None:
        self.database = str(database)

    def record_bid(self, *, operation_id: str, user_id: str, bid_price: int, occurred_at: str) -> bool:
        user_id = str(user_id)
        operation_id = str(operation_id)
        increments = (("拍卖出价次数", 1), ("拍卖出价灵石", int(bid_price)))
        recorded = False
        with DatabaseUnitOfWork(self.database, immediate=True) as uow:
            for event_key, increment in increments:
                old = uow.query_one(
                    "SELECT user_id,increment FROM auction_bid_statistics_events "
                    "WHERE operation_id=? AND event_key=?",
                    (operation_id, event_key),
                )
                if old is not None:
                    if str(old["user_id"]) != user_id or int(old["increment"]) != increment:
                        raise OperationConflictError(operation_id, "auction.bid.statistics")
                    continue
                recorded = True
                uow.execute(
                    "INSERT INTO auction_bid_statistics_events "
                    "(operation_id,event_key,user_id,increment,created_at) VALUES (?,?,?,?,?)",
                    (operation_id, event_key, user_id, increment, str(occurred_at)),
                )
                uow.execute(
                    f'INSERT INTO statistics(user_id,"{event_key}") VALUES (?,?) '
                    f'ON CONFLICT(user_id) DO UPDATE SET "{event_key}"='
                    f'COALESCE(statistics."{event_key}",0)+excluded."{event_key}"',
                    (user_id, increment),
                )
        return recorded

    def values(self, *, user_id: str) -> dict[str, int]:
        with DatabaseUnitOfWork(self.database) as uow:
            row = uow.query_one(
                'SELECT "拍卖出价次数","拍卖出价灵石" FROM statistics WHERE user_id=?',
                (str(user_id),),
            )
        return {key: int((row or {}).get(key) or 0) for key in self._METRICS}


__all__ = ["AuctionBidStatisticsRepository"]
