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
from ...core.errors import DomainError
from .guishi_match_repository import GuishiMatchResult
from .guishi_immediate_match import GuishiMatchFailure

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
            row = uow.query_one(
                "SELECT id,user_id,item_id,item_name,item_type,price,quantity,"
                "COALESCE(filled_quantity,0) AS filled_quantity FROM guishi_item WHERE id=?",
                (baitan_order_id,),
            )
        if row is None:
            return None
        if str(row["item_type"]) not in _BAITAN_TYPES:
            raise GuishiMatchFailure("baitan_invalid", "", baitan_order_id)
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
            try:
                listing = self._listing(order_id)
            except GuishiMatchFailure as exc:
                exc.matches = list(matches)
                raise
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
                result = self._match_pair(
                    operation_id=f"guishi-fill:{order_id}:{qiugou_order_id}",
                    qiugou_order_id=qiugou_order_id,
                    baitan_order_id=order_id,
                    matches=matches,
                )
                if result.matched:
                    if result.status != "duplicate":
                        matches.append(result)
                    progressed = True
                    break
                if result.status not in {"order_missing", "qiugou_completed", "baitan_completed", "not_matchable"}:
                    raise GuishiMatchFailure(result.status, qiugou_order_id, order_id, matches=matches)
            if not progressed:
                break
        return matches

    def _match_pair(self, *, operation_id: str, qiugou_order_id: str,
                    baitan_order_id: str, matches: list[GuishiMatchResult]) -> GuishiMatchResult:
        try:
            return self.match(operation_id=operation_id, qiugou_order_id=qiugou_order_id,
                              baitan_order_id=baitan_order_id)
        except DomainError as exc:
            raise GuishiMatchFailure(exc.details.get("reason") or exc.code, qiugou_order_id,
                                     baitan_order_id, matches=matches,
                                     subject=exc.details.get("subject", "")) from exc

    def fill_buy(self, *, qiugou_order_id: str) -> list[GuishiMatchResult]:
        """Resume only this buy order, using the existing sweep price ordering."""
        order_id = str(qiugou_order_id)
        matches: list[GuishiMatchResult] = []
        attempted: set[str] = set()
        while True:
            with DatabaseUnitOfWork(self.trade_database, read_only=True) as uow:
                buy = uow.query_one("SELECT * FROM guishi_item WHERE id=?", (order_id,))
                if buy is None:
                    return matches
                if str(buy["item_type"]) not in _QIUGOU_TYPES:
                    raise GuishiMatchFailure("qiugou_invalid", order_id, "", matches=matches)
                sells = uow.query_all(
                    "SELECT id,price FROM guishi_item WHERE item_type IN ('baitan','摆摊') AND item_name=? "
                    "AND user_id<>? ORDER BY id",
                    (buy["item_name"], buy["user_id"]),
                )
                sells = sorted((row for row in sells if int(row["price"]) <= int(buy["price"])),
                               key=lambda row: int(row["price"]))
            progressed = False
            for sell in sells:
                sell_id = str(sell["id"])
                if sell_id in attempted:
                    continue
                attempted.add(sell_id)
                result = self._match_pair(operation_id=f"guishi-match:{order_id}:{sell_id}",
                                          qiugou_order_id=order_id, baitan_order_id=sell_id, matches=matches)
                if result.matched:
                    if result.status != "duplicate":
                        matches.append(result)
                    if result.qiugou_completed:
                        return matches
                    progressed = True
                    break
                if result.status == "qiugou_completed":
                    return matches
                if result.status not in {"order_missing", "baitan_completed", "not_matchable"}:
                    raise GuishiMatchFailure(result.status, order_id, sell_id, matches=matches)
            if not progressed:
                return matches


__all__ = ["GuishiListingFillService"]
