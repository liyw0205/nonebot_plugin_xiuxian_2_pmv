from __future__ import annotations

import json
from pathlib import Path
from typing import Any, Mapping, Protocol

from ...infrastructure.clock import SystemClock
from ...infrastructure.database import DatabaseUnitOfWork


class AuctionQueryRepository(Protocol):
    def get_current_auction(self, auction_id: str | None = None) -> Any: ...

    def count_current_auctions(self) -> int: ...

    def get_auction_history(self, auction_id: str | None = None) -> list[dict[str, Any]]: ...

    def get_recent_auction_deals(self, limit: int = 5) -> list[dict[str, Any]]: ...

    def count_auction_history(self) -> int: ...

    def my_bids(self, user_id: str) -> list[dict[str, Any]]: ...

    def my_auction_snapshot(
        self, user_id: str, *, limit: int = 5, now_epoch: float | None = None
    ) -> dict[str, Any]: ...


class AuctionQuerySqlRepository:
    """Read the legacy auction projection without creating its schema."""

    def __init__(self, database: str | Path, *, clock: Any | None = None) -> None:
        self.database = Path(database)
        self.clock = clock or SystemClock()

    @staticmethod
    def _table_exists(uow: DatabaseUnitOfWork, name: str) -> bool:
        return uow.query_one(
            "SELECT 1 AS present FROM sqlite_master WHERE type='table' AND name=?",
            (name,),
        ) is not None

    @staticmethod
    def _auction_row(row: Mapping[str, Any]) -> dict[str, Any]:
        result = dict(row)
        for field in ("bids", "bid_times"):
            try:
                value = json.loads(result.get(field) or "{}")
                result[field] = value if isinstance(value, dict) else {}
            except (TypeError, ValueError):
                result[field] = {}
        result["is_system"] = bool(result.get("is_system"))
        return result

    def get_current_auction(self, auction_id: str | None = None):
        if not self.database.is_file():
            return None if auction_id is not None else []
        with DatabaseUnitOfWork(self.database, read_only=True) as uow:
            if not self._table_exists(uow, "auction_current"):
                return None if auction_id is not None else []
            if auction_id is None:
                return [
                    self._auction_row(row)
                    for row in uow.query_all("SELECT * FROM auction_current")
                ]
            row = uow.query_one(
                "SELECT * FROM auction_current WHERE id=?", (str(auction_id),)
            )
            return self._auction_row(row) if row else None

    def count_current_auctions(self) -> int:
        if not self.database.is_file():
            return 0
        with DatabaseUnitOfWork(self.database, read_only=True) as uow:
            if not self._table_exists(uow, "auction_current"):
                return 0
            row = uow.query_one("SELECT COUNT(*) AS total FROM auction_current")
            return int(row["total"]) if row else 0

    def get_auction_history(self, auction_id: str | None = None) -> list[dict[str, Any]]:
        if not self.database.is_file():
            return []
        with DatabaseUnitOfWork(self.database, read_only=True) as uow:
            if not self._table_exists(uow, "auction_history"):
                return []
            if auction_id is None:
                return uow.query_all(
                    "SELECT * FROM auction_history ORDER BY end_time DESC"
                )
            return uow.query_all(
                "SELECT * FROM auction_history WHERE auction_id=? ORDER BY end_time DESC",
                (str(auction_id),),
            )

    def get_recent_auction_deals(self, limit: int = 5) -> list[dict[str, Any]]:
        if not self.database.is_file():
            return []
        limit = max(int(limit), 0)
        if limit == 0:
            return []
        with DatabaseUnitOfWork(self.database, read_only=True) as uow:
            if not self._table_exists(uow, "auction_history"):
                return []
            return uow.query_all(
                "SELECT * FROM auction_history "
                "WHERE status=? AND final_price IS NOT NULL "
                "ORDER BY end_time DESC LIMIT ?",
                ("成交", limit),
            )

    def count_auction_history(self) -> int:
        if not self.database.is_file():
            return 0
        with DatabaseUnitOfWork(self.database, read_only=True) as uow:
            if not self._table_exists(uow, "auction_history"):
                return 0
            row = uow.query_one("SELECT COUNT(*) AS total FROM auction_history")
            return int(row["total"]) if row else 0

    def my_bids(self, user_id: str) -> list[dict[str, Any]]:
        """Return the player's bids on still-active auctions (read-only).

        The bid repository drops a player from ``auction_current.bids`` as soon as
        somebody outbids them, so the standing bid is only recoverable from the
        bid operation ledger; ``bids`` supplies the current top for the leader flag.
        """

        if not self.database.is_file():
            return []
        user_id = str(user_id or "").strip()
        if not user_id:
            return []
        with DatabaseUnitOfWork(self.database, read_only=True) as uow:
            if not self._table_exists(uow, "auction_current"):
                return []
            auctions = {
                str(row["id"]): self._auction_row(row)
                for row in uow.query_all("SELECT * FROM auction_current ORDER BY id")
            }
            latest_bid: dict[str, int] = {}
            if self._table_exists(uow, "auction_bid_operations"):
                for row in uow.query_all(
                    "SELECT auction_id,bid_price FROM auction_bid_operations "
                    "WHERE bidder_id=? ORDER BY rowid",
                    (user_id,),
                ):
                    latest_bid[str(row["auction_id"])] = int(row["bid_price"] or 0)
        bids: list[dict[str, Any]] = []
        for auction_id, record in auctions.items():
            prices = record.get("bids") or {}
            leading_price = int(prices.get(user_id) or 0)
            my_price = leading_price or latest_bid.get(auction_id, 0)
            if my_price <= 0:
                continue
            top = max((int(value) for value in prices.values()), default=0)
            bids.append(
                {
                    "auction_id": auction_id,
                    "item_id": int(record.get("item_id") or 0),
                    "item_name": str(record.get("name") or ""),
                    "my_price": my_price,
                    "current_price": max(int(record.get("current_price") or 0), top),
                    "leading": leading_price > 0 and leading_price >= top,
                    "bid_count": len(prices),
                }
            )
        return bids

    def my_auction_snapshot(
        self, user_id: str, *, limit: int = 5, now_epoch: float | None = None
    ) -> dict[str, Any]:
        """Read the player's live lots and settled records in one snapshot.

        ``auction_current`` only holds the running session, so ``lots`` is
        naturally scoped to "this auction".  Settlement deletes those rows and
        writes ``auction_history``, which is why the durable outcome has to be
        read from history instead of the live table.
        """

        empty: dict[str, Any] = {"lots": [], "records": []}
        if not self.database.is_file():
            return empty
        user_id = str(user_id or "").strip()
        if not user_id:
            return empty
        limit = max(int(limit), 0)
        moment = float(now_epoch) if now_epoch is not None else float(
            self.clock.now().timestamp()
        )

        with DatabaseUnitOfWork(self.database, read_only=True) as uow:
            if not self._table_exists(uow, "auction_current"):
                return empty
            end_time = None
            if self._table_exists(uow, "auction_sessions"):
                row = uow.query_one(
                    "SELECT end_time FROM auction_sessions WHERE status='active'"
                )
                if row is not None:
                    end_time = float(row["end_time"])
            lot_rows = uow.query_all(
                "SELECT id,item_id,name,start_price,current_price,seller_id,bids "
                "FROM auction_current WHERE seller_id=? AND is_system=0 ORDER BY id",
                (user_id,),
            )
            lot_ids = [str(row["id"]) for row in lot_rows]
            bid_stats: dict[str, tuple[int, int]] = {}
            if lot_ids and self._table_exists(uow, "auction_bid_operations"):
                placeholders = ",".join("?" for _ in lot_ids)
                for row in uow.query_all(
                    "SELECT auction_id,COUNT(*) AS total,"
                    "COUNT(DISTINCT bidder_id) AS bidders "
                    f"FROM auction_bid_operations WHERE auction_id IN ({placeholders}) "
                    "GROUP BY auction_id",
                    tuple(lot_ids),
                ):
                    bid_stats[str(row["auction_id"])] = (
                        int(row["total"] or 0),
                        int(row["bidders"] or 0),
                    )
            records: list[dict[str, Any]] = []
            if self._table_exists(uow, "auction_history"):
                for row in uow.query_all(
                    "SELECT auction_id,item_id,item_name,final_price,seller_id,seller_name,"
                    "winner_id,winner_name,status,fee,seller_earnings,end_time "
                    "FROM auction_history WHERE seller_id=? OR winner_id=? "
                    "ORDER BY end_time DESC LIMIT ?",
                    (user_id, user_id, limit),
                ):
                    seller = str(row["seller_id"]) == user_id
                    counterparty = (
                        str(row["winner_name"] or row["winner_id"] or "未知")
                        if seller
                        else str(row["seller_name"] or row["seller_id"] or "未知")
                    )
                    records.append(
                        {
                            "auction_id": str(row["auction_id"]),
                            "item_id": int(row["item_id"] or 0),
                            "item_name": str(row["item_name"] or ""),
                            "role": "seller" if seller else "winner",
                            "status": str(row["status"] or ""),
                            "final_price": (
                                None
                                if row["final_price"] is None
                                else int(row["final_price"])
                            ),
                            "fee": int(row["fee"] or 0),
                            "seller_earnings": int(row["seller_earnings"] or 0),
                            "counterparty": counterparty,
                            "end_time": float(row["end_time"] or 0.0),
                        }
                    )

        lots: list[dict[str, Any]] = []
        for row in lot_rows:
            auction_id = str(row["id"])
            try:
                prices = json.loads(str(row["bids"] or "{}"))
            except (TypeError, ValueError):
                prices = {}
            if not isinstance(prices, dict):
                prices = {}
            top = max((int(value) for value in prices.values()), default=0)
            total, bidders = bid_stats.get(auction_id, (len(prices), len(prices)))
            lots.append(
                {
                    "auction_id": auction_id,
                    "item_id": int(row["item_id"] or 0),
                    "item_name": str(row["name"] or ""),
                    "start_price": int(row["start_price"] or 0),
                    "current_price": max(int(row["current_price"] or 0), top),
                    "bid_count": total,
                    "bidder_count": bidders,
                    "remaining_seconds": (
                        None if end_time is None else max(int(end_time - moment), 0)
                    ),
                }
            )
        return {"lots": lots, "records": records}


__all__ = ["AuctionQueryRepository", "AuctionQuerySqlRepository"]
