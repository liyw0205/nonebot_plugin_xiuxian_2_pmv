"""Sweep expired Guishi buy orders (求购) and release their frozen stones.

The player-facing promise is "次日 8 点自动取消订单，并退还未购得物品的灵石".
This service is the only expiry path: it reads the orders that provably passed
their deadline and delegates each cancellation to the existing atomic cancel
repository, so the refund arithmetic stays in one place.

Design notes:

* The creation instant comes from ``guishi_order_create_operations`` (written in
  the same transaction as the order). Orders without that row are historical and
  never auto-expire — the sweep must not guess a time and delete a player's order.
* The operation id is derived from the order id, so a repeated sweep (job plus a
  player-triggered convergence) replays as ``duplicate`` and never refunds twice.
* A missing table or database is a no-op, never a request-time DDL.
"""

from __future__ import annotations

from pathlib import Path
from typing import Any, Callable

from ...infrastructure.clock import SystemClock
from ...infrastructure.database import DatabaseUnitOfWork
from .guishi_cancel_repository import GuishiOrderCancelResult
from .guishi_lifecycle import (
    DEFAULT_GUISHI_END_HOUR,
    parse_created_at,
    qiugou_is_expired,
)

_QIUGOU_TYPES = ("qiugou", "求购")


class GuishiQiugouExpiryService:
    """Cancel expired buy orders and refund their unfilled frozen balance."""

    def __init__(
        self,
        trade_database: str | Path,
        *,
        cancel: Callable[..., GuishiOrderCancelResult],
        clock: Any | None = None,
        end_hour: int = DEFAULT_GUISHI_END_HOUR,
    ) -> None:
        self.trade_database = str(trade_database)
        self.cancel = cancel
        self.clock = clock or SystemClock()
        self.end_hour = int(end_hour)

    @staticmethod
    def _table_exists(uow: DatabaseUnitOfWork, table: str) -> bool:
        return (
            uow.query_one(
                "SELECT 1 AS present FROM sqlite_master WHERE type='table' AND name=?",
                (table,),
            )
            is not None
        )

    def _candidates(self, user_id: str | None) -> list[dict[str, Any]]:
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
                # No anchor table: keep every order, do not create schema here.
                created_at_sql = "NULL"
            sql = (
                "SELECT g.id AS id,g.user_id AS user_id,"
                f"{created_at_sql} AS created_at FROM guishi_item g "
                "WHERE g.item_type IN ('qiugou','求购')"
            )
            params: list[str] = []
            if user_id is not None:
                sql += " AND g.user_id=?"
                params.append(str(user_id))
            return [dict(row) for row in uow.query_all(sql, params) or []]

    def expire(
        self, *, user_id: str | None = None, now: Any | None = None
    ) -> list[GuishiOrderCancelResult]:
        """Cancel every provably expired buy order (optionally scoped to one player)."""

        moment = now if now is not None else self.clock.now()
        expired: list[dict[str, Any]] = []
        for row in self._candidates(user_id):
            created_at = parse_created_at(row.get("created_at"))
            if qiugou_is_expired(created_at, moment, end_hour=self.end_hour):
                expired.append(row)

        results: list[GuishiOrderCancelResult] = []
        for row in expired:
            order_id = str(row.get("id") or "")
            owner = str(row.get("user_id") or "")
            if not order_id or not owner:
                continue
            results.append(
                self.cancel(
                    operation_id=f"guishi-expire-qiugou:{order_id}",
                    user_id=owner,
                    order_id=order_id,
                )
            )
        return results


__all__ = ["GuishiQiugouExpiryService"]
