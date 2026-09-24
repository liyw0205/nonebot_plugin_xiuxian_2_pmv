from __future__ import annotations

import json
from pathlib import Path
from typing import Any, Mapping, Protocol

from ...infrastructure.database import DatabaseUnitOfWork


class AuctionQueryRepository(Protocol):
    def get_current_auction(self, auction_id: str | None = None) -> Any: ...

    def count_current_auctions(self) -> int: ...

    def get_auction_history(self, auction_id: str | None = None) -> list[dict[str, Any]]: ...

    def count_auction_history(self) -> int: ...


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

    def count_auction_history(self) -> int:
        if not self.database.is_file():
            return 0
        with DatabaseUnitOfWork(self.database, read_only=True) as uow:
            if not self._table_exists(uow, "auction_history"):
                return 0
            row = uow.query_one("SELECT COUNT(*) AS total FROM auction_history")
            return int(row["total"]) if row else 0


__all__ = ["AuctionQueryRepository", "AuctionQuerySqlRepository"]
