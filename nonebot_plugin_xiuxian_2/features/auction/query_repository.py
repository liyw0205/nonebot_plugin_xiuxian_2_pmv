from __future__ import annotations

import json
from pathlib import Path
from typing import Any, Mapping, Protocol

from ...infrastructure.database import DatabaseUnitOfWork


class AuctionQueryRepository(Protocol):
    def get_current_auction(self, auction_id: str | None = None) -> Any: ...

    def count_current_auctions(self) -> int: ...

    def get_auction_history(self, auction_id: str | None = None) -> list[dict[str, Any]]: ...

    def get_recent_auction_deals(self, limit: int = 5) -> list[dict[str, Any]]: ...

    def count_auction_history(self) -> int: ...

    def my_bids(self, user_id: str) -> list[dict[str, Any]]: ...


class AuctionQuerySqlRepository:
    """Read the legacy auction projection without creating its schema."""

    def __init__(self, database: str | Path) -> None:
        self.database = Path(database)

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


__all__ = ["AuctionQueryRepository", "AuctionQuerySqlRepository"]
