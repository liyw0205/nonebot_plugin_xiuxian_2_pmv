from __future__ import annotations

import json
import secrets
from dataclasses import dataclass
from pathlib import Path
from typing import Protocol

from ...infrastructure.database import DatabaseUnitOfWork


class OrderIdGenerator(Protocol):
    def new_id(self) -> str: ...


class GuishiOrderIdGenerator:
    """Generate numeric IDs compatible with the historical Guishi commands."""

    def new_id(self) -> str:
        return str(secrets.randbelow(9_000_000_000_000) + 1_000_000_000_000)


@dataclass(frozen=True)
class GuishiQiugouCreateResult:
    status: str
    order_id: str = ""
    total_cost: int = 0

    @property
    def created(self) -> bool:
        return self.status in {"created", "duplicate"}

    @property
    def applied(self) -> bool:
        return self.status == "created"


class GuishiQiugouSqlRepository:
    """Create a Guishi buy order while freezing its stone balance atomically."""

    def __init__(
        self,
        trade_database: str | Path,
        *,
        order_ids: OrderIdGenerator | None = None,
    ) -> None:
        self.trade_database = str(trade_database)
        self.order_ids = order_ids or GuishiOrderIdGenerator()

    @staticmethod
    def _payload(
        user_id: str, item_id: int, item_name: str, price: int, quantity: int, max_orders: int
    ) -> str:
        # Keep the historical list payload so operations created by the old
        # repository replay without a second balance deduction.
        return json.dumps(
            [user_id, item_id, item_name, price, quantity, max_orders],
            ensure_ascii=False,
        )

    def create(
        self,
        *,
        operation_id: str,
        user_id: str,
        item_id: int,
        item_name: str,
        price: int,
        quantity: int,
        max_orders: int,
    ) -> GuishiQiugouCreateResult:
        operation_id = str(operation_id or "").strip()
        user_id = str(user_id)
        item_id = int(item_id)
        item_name = str(item_name)
        price = max(int(price), 0)
        quantity = max(int(quantity), 1)
        max_orders = max(int(max_orders), 1)
        total_cost = price * quantity
        payload = self._payload(user_id, item_id, item_name, price, quantity, max_orders)

        with DatabaseUnitOfWork(self.trade_database, immediate=True) as uow:
            if operation_id:
                previous = uow.query_one(
                    "SELECT payload,order_id,order_type,amount "
                    "FROM guishi_order_create_operations WHERE operation_id=?",
                    (operation_id,),
                )
                if previous is not None:
                    if (
                        str(previous["payload"]) != payload
                        or str(previous["order_type"]) != "qiugou"
                    ):
                        return GuishiQiugouCreateResult(
                            "operation_conflict", total_cost=total_cost
                        )
                    return GuishiQiugouCreateResult(
                        "duplicate", str(previous["order_id"]), int(previous["amount"])
                    )

            count = uow.query_one(
                "SELECT COUNT(*) AS count FROM guishi_item "
                "WHERE user_id=? AND (item_type=? OR item_type=?)",
                (user_id, "qiugou", "求购"),
            )
            if int(count["count"]) >= max_orders:
                return GuishiQiugouCreateResult("limit_reached", total_cost=total_cost)

            balance = uow.query_one(
                "SELECT COALESCE(stored_stone,0) AS stored_stone "
                "FROM guishi_info WHERE user_id=?",
                (user_id,),
            )
            if balance is None or int(balance["stored_stone"]) < total_cost:
                return GuishiQiugouCreateResult("stone_insufficient", total_cost=total_cost)

            order_id = ""
            for _ in range(20):
                candidate = str(self.order_ids.new_id()).strip()
                if not candidate:
                    continue
                try:
                    uow.execute(
                        "INSERT INTO guishi_item("
                        "id,user_id,item_id,item_name,item_type,price,quantity) "
                        "VALUES(?,?,?,?,?,?,?)",
                        (
                            candidate,
                            user_id,
                            item_id,
                            item_name,
                            "qiugou",
                            price,
                            quantity,
                        ),
                    )
                    order_id = candidate
                    break
                except Exception as exc:
                    # Keep the repository independent from the SQLite driver;
                    # only an order-id uniqueness race is retryable.
                    if "UNIQUE constraint failed: guishi_item.id" not in str(exc):
                        raise
                    if uow.query_one("SELECT 1 AS present FROM guishi_item WHERE id=?", (candidate,)):
                        continue
                    raise
            if not order_id:
                raise RuntimeError("failed to allocate guishi order id")

            charged = uow.execute(
                "UPDATE guishi_info SET stored_stone=CAST(COALESCE(stored_stone,0) AS REAL)-CAST(? AS REAL) "
                "WHERE user_id=? AND COALESCE(stored_stone,0)>=?",
                (total_cost, user_id, total_cost),
            )
            if charged.rowcount != 1:
                return GuishiQiugouCreateResult("state_changed", total_cost=total_cost)
            if operation_id:
                uow.execute(
                    "INSERT INTO guishi_order_create_operations("
                    "operation_id,payload,order_id,order_type,amount) VALUES(?,?,?,?,?)",
                    (operation_id, payload, order_id, "qiugou", total_cost),
                )
            return GuishiQiugouCreateResult("created", order_id, total_cost)


__all__ = [
    "GuishiOrderIdGenerator",
    "GuishiQiugouCreateResult",
    "GuishiQiugouSqlRepository",
    "OrderIdGenerator",
]
