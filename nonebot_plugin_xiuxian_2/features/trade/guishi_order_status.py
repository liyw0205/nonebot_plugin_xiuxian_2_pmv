"""Read one player's Guishi orders with their lifecycle anchors.

``鬼市信息`` needs three facts per order: what it is, how much is left, and when it
expires. The order row carries the first two and the creation operation row carries
the third, so this service joins them in one read-only pass and hands the result to
the pure renderer in :mod:`guishi_order_view`.
"""

from __future__ import annotations

from datetime import datetime
from pathlib import Path
from typing import Any

from ...infrastructure.clock import SystemClock
from ...infrastructure.database import DatabaseUnitOfWork
from .guishi_lifecycle import DEFAULT_GUISHI_END_HOUR
from .guishi_order_view import order_status_lines

_QIUGOU_TYPES = ("qiugou", "求购")


class GuishiOrderStatusService:
    """Render a player's own buy orders and stalls with deadline information."""

    def __init__(
        self,
        trade_database: str | Path,
        *,
        clock: Any | None = None,
        end_hour: int = DEFAULT_GUISHI_END_HOUR,
    ) -> None:
        self.trade_database = str(trade_database)
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

    def _snapshot(
        self, user_id: str
    ) -> tuple[list[dict[str, Any]], dict[str, Any]]:
        database = Path(self.trade_database)
        if not database.is_file():
            return [], {}
        with DatabaseUnitOfWork(database, read_only=True) as uow:
            if not self._table_exists(uow, "guishi_item"):
                return [], {}
            orders = [
                dict(row)
                for row in uow.query_all(
                    "SELECT id,user_id,item_id,item_name,item_type,price,quantity,"
                    "COALESCE(filled_quantity,0) AS filled_quantity FROM guishi_item "
                    "WHERE user_id=? ORDER BY id",
                    (str(user_id),),
                )
                or []
            ]
            if not orders or not self._table_exists(
                uow, "guishi_order_create_operations"
            ):
                return orders, {}
            anchors = {
                str(row["order_id"]): row["created_at"]
                for row in uow.query_all(
                    "SELECT order_id,MAX(created_at) AS created_at "
                    "FROM guishi_order_create_operations "
                    "WHERE order_type IN ('qiugou','求购','baitan','摆摊') "
                    "GROUP BY order_id"
                )
                or []
            }
        return orders, anchors

    def lines_for(
        self, user_id: str, *, now: datetime | None = None
    ) -> dict[str, list[str]]:
        """Return ``{"qiugou": [...], "baitan": [...]}`` rendered status lines."""

        moment = now if now is not None else self.clock.now()
        orders, anchors = self._snapshot(str(user_id))
        qiugou: list[dict[str, Any]] = []
        baitan: list[dict[str, Any]] = []
        for order in orders:
            if str(order.get("item_type") or "") in _QIUGOU_TYPES:
                qiugou.append(order)
            else:
                baitan.append(order)
        return {
            "qiugou": order_status_lines(
                qiugou, anchors, now=moment, end_hour=self.end_hour
            ),
            "baitan": order_status_lines(
                baitan, anchors, now=moment, end_hour=self.end_hour
            ),
        }


__all__ = ["GuishiOrderStatusService"]
