"""Fill standing Guishi buy orders (求购) as soon as a stall is listed.

``鬼市求购`` already triggers ``process_guishi_transactions`` right after creating the
order, but ``鬼市摆摊`` did nothing: a seller whose item matched an existing buy
order had to wait for the next 02:00 sweep, and the match never reached them.

This service closes that asymmetry without inventing a second matching rule — it
selects the standing buy orders a fresh stall can satisfy and delegates every pair
to the existing atomic match repository, which owns the price gate, partial fills
and operation replay.

Selection is FIFO (oldest buy order first, anchored on the creation operation row)
so an early requester is not starved by a later, higher bid: the execution price is
the stall price either way.
"""

from __future__ import annotations

from pathlib import Path
from typing import Any, Callable

from ...infrastructure.clock import SystemClock
from ...infrastructure.database import DatabaseUnitOfWork
from .guishi_match_repository import GuishiMatchResult

_QIUGOU_TYPES = ("qiugou", "求购")
_BAITAN_TYPES = ("baitan", "摆摊")


class GuishiListingFillService:
    """Match one stall against the buy orders it can satisfy, oldest first."""

    def __init__(
        self,
        trade_database: str | Path,
        *,
        match: Callable[..., GuishiMatchResult],
        clock: Any | None = None,
    ) -> None:
        self.trade_database = str(trade_database)
        self.match = match
        self.clock = clock or SystemClock()

    @staticmethod
    def _table_exists(uow: DatabaseUnitOfWork, table: str) -> bool:
        return (
            uow.query_one(
                "SELECT 1 AS present FROM sqlite_master WHERE type='table' AND name=?",
                (table,),
            )
            is not None
        )

    def _listing(self, baitan_order_id: str) -> dict[str, Any] | None:
        database = Path(self.trade_database)
        if not database.is_file():
            return None
        with DatabaseUnitOfWork(database, read_only=True) as uow:
            if not self._table_exists(uow, "guishi_item"):
                return None
            row = uow.query_one(
                "SELECT id,user_id,item_id,item_name,item_type,price,quantity,"
                "COALESCE(filled_quantity,0) AS filled_quantity FROM guishi_item WHERE id=?",
                (baitan_order_id,),
            )
        if row is None or str(row["item_type"]) not in _BAITAN_TYPES:
            return None
        return dict(row)

    def _candidates(self, listing: dict[str, Any]) -> list[dict[str, Any]]:
        database = Path(self.trade_database)
        if not database.is_file():
            return []
        with DatabaseUnitOfWork(database, read_only=True) as uow:
            if not self._table_exists(uow, "guishi_item"):
                return []
            if self._table_exists(uow, "guishi_order_create_operations"):
                created_at_sql = (
                    "(SELECT MAX(o.created_at) FROM guishi_order_create_operations o "
                    "WHERE o.order_id=g.id AND o.order_type IN ('qiugou','求购'))"
                )
            else:
                created_at_sql = "NULL"
            rows = uow.query_all(
                "SELECT g.id AS id,g.user_id AS user_id,g.price AS price,"
                "g.quantity AS quantity,COALESCE(g.filled_quantity,0) AS filled_quantity,"
                f"{created_at_sql} AS created_at FROM guishi_item g "
                "WHERE g.item_type IN ('qiugou','求购') AND g.item_name=? "
                "AND g.user_id<>? AND g.price>=? "
                "AND (g.quantity-COALESCE(g.filled_quantity,0))>0 "
                "ORDER BY (created_at IS NULL) ASC,created_at ASC,g.id ASC",
                (
                    str(listing.get("item_name") or ""),
                    str(listing.get("user_id") or ""),
                    int(listing.get("price") or 0),
                ),
            )
            return [dict(row) for row in rows or []]

    def fill(self, *, baitan_order_id: str) -> list[GuishiMatchResult]:
        """Match the stall until it is sold out or no buy order can take it."""

        order_id = str(baitan_order_id or "").strip()
        if not order_id:
            return []
        matches: list[GuishiMatchResult] = []
        attempted: set[str] = set()
        while True:
            listing = self._listing(order_id)
            if listing is None:
                break
            remaining = max(
                int(listing.get("quantity") or 0)
                - int(listing.get("filled_quantity") or 0),
                0,
            )
            if remaining <= 0:
                break
            progressed = False
            for candidate in self._candidates(listing):
                qiugou_order_id = str(candidate.get("id") or "")
                if not qiugou_order_id or qiugou_order_id in attempted:
                    continue
                attempted.add(qiugou_order_id)
                result = self.match(
                    operation_id=f"guishi-fill:{order_id}:{qiugou_order_id}",
                    qiugou_order_id=qiugou_order_id,
                    baitan_order_id=order_id,
                )
                if result.matched:
                    matches.append(result)
                    progressed = True
                    break
            if not progressed:
                break
        return matches


__all__ = ["GuishiListingFillService"]
