from __future__ import annotations

import json
import secrets
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Protocol

from ...infrastructure.clock import SystemClock
from ...infrastructure.database import DatabaseUnitOfWork


class OrderIdGenerator(Protocol):
    def new_id(self) -> str: ...


class GuishiBaitanOrderIdGenerator:
    """Generate numeric IDs compatible with the historical Guishi commands."""

    def new_id(self) -> str:
        return str(secrets.randbelow(9_000_000_000_000) + 1_000_000_000_000)


@dataclass(frozen=True)
class GuishiBaitanCreateResult:
    status: str
    order_id: str = ""
    quantity: int = 0

    @property
    def created(self) -> bool:
        return self.status in {"created", "duplicate"}

    @property
    def applied(self) -> bool:
        return self.status == "created"


class GuishiBaitanSqlRepository:
    """Create a Guishi sell order while reserving tradeable inventory atomically."""

    def __init__(
        self,
        game_database: str | Path,
        trade_database: str | Path,
        *,
        order_ids: OrderIdGenerator | None = None,
        clock: Any | None = None,
    ) -> None:
        self.game_database = str(game_database)
        self.trade_database = str(trade_database)
        self.order_ids = order_ids or GuishiBaitanOrderIdGenerator()
        self.clock = clock or SystemClock()

    @staticmethod
    def _payload(
        user_id: str, item_id: int, item_name: str, price: int, quantity: int, max_orders: int
    ) -> str:
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
    ) -> GuishiBaitanCreateResult:
        operation_id = str(operation_id or "").strip()
        user_id = str(user_id).strip()
        item_id = int(item_id)
        item_name = str(item_name)
        price = max(int(price), 0)
        quantity = max(int(quantity), 1)
        max_orders = max(int(max_orders), 1)
        payload = self._payload(user_id, item_id, item_name, price, quantity, max_orders)

        with DatabaseUnitOfWork(self.game_database, immediate=True) as uow:
            uow.attach_database(self.trade_database, "guishi_trade")
            if operation_id:
                previous = uow.query_one(
                    "SELECT payload,order_id,order_type,amount "
                    "FROM guishi_trade.guishi_order_create_operations WHERE operation_id=?",
                    (operation_id,),
                )
                if previous is not None:
                    if (
                        str(previous["payload"]) != payload
                        or str(previous["order_type"]) != "baitan"
                    ):
                        return GuishiBaitanCreateResult("operation_conflict", quantity=quantity)
                    return GuishiBaitanCreateResult(
                        "duplicate", str(previous["order_id"]), int(previous["amount"])
                    )

            count = uow.query_one(
                "SELECT COUNT(*) AS count FROM guishi_trade.guishi_item "
                "WHERE user_id=? AND (item_type=? OR item_type=?)",
                (user_id, "baitan", "摆摊"),
            )
            if int(count["count"]) >= max_orders:
                return GuishiBaitanCreateResult("limit_reached", quantity=quantity)

            inventory = uow.query_one(
                "SELECT COALESCE(goods_num,0) AS goods_num, "
                "COALESCE(bind_num,0) AS bind_num, COALESCE(state,0) AS state "
                "FROM back WHERE user_id=? AND goods_id=?",
                (user_id, item_id),
            )
            if inventory is None or int(inventory["goods_num"]) - int(inventory["state"]) < quantity:
                return GuishiBaitanCreateResult("stock_insufficient", quantity=quantity)

            updated = uow.execute(
                "UPDATE back SET goods_num=goods_num-?, "
                "bind_num=MIN(COALESCE(bind_num,0),goods_num-?), update_time=? "
                "WHERE user_id=? AND goods_id=? "
                "AND COALESCE(goods_num,0)-COALESCE(state,0)>=?",
                (
                    quantity,
                    quantity,
                    self.clock.now().isoformat(),
                    user_id,
                    item_id,
                    quantity,
                ),
            )
            if updated.rowcount != 1:
                return GuishiBaitanCreateResult("state_changed", quantity=quantity)

            order_id = ""
            for _ in range(20):
                candidate = str(self.order_ids.new_id()).strip()
                if not candidate:
                    continue
                try:
                    uow.execute(
                        "INSERT INTO guishi_trade.guishi_item("
                        "id,user_id,item_id,item_name,item_type,price,quantity) "
                        "VALUES(?,?,?,?,?,?,?)",
                        (candidate, user_id, item_id, item_name, "baitan", price, quantity),
                    )
                    order_id = candidate
                    break
                except Exception as exc:
                    if "UNIQUE constraint failed: guishi_item.id" not in str(exc):
                        raise
                    if uow.query_one(
                        "SELECT 1 AS present FROM guishi_trade.guishi_item WHERE id=?",
                        (candidate,),
                    ):
                        continue
                    raise
            if not order_id:
                raise RuntimeError("failed to allocate guishi order id")

            if operation_id:
                uow.execute(
                    "INSERT INTO guishi_trade.guishi_order_create_operations("
                    "operation_id,payload,order_id,order_type,amount) VALUES(?,?,?,?,?)",
                    (operation_id, payload, order_id, "baitan", quantity),
                )
            return GuishiBaitanCreateResult("created", order_id, quantity)


__all__ = [
    "GuishiBaitanCreateResult",
    "GuishiBaitanOrderIdGenerator",
    "GuishiBaitanSqlRepository",
    "OrderIdGenerator",
]
