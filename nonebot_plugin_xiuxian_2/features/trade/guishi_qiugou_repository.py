from __future__ import annotations

from ...core.numeric import as_int_like
from ...core.errors import DomainError
import json
import secrets
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Protocol

from ...infrastructure.clock import SystemClock
from ...infrastructure.database import DatabaseUnitOfWork
from .guishi_operation_replay import order_receipts
from .guishi_immediate_match import enqueue_immediate_match


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
    reason: str = ""
    subject: str = ""

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
        clock: Any | None = None,
    ) -> None:
        self.trade_database = str(trade_database)
        self.order_ids = order_ids or GuishiOrderIdGenerator()
        self.clock = clock or SystemClock()

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
        immediate_match: bool = False,
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
                receipts = order_receipts(
                    uow, "guishi_order_create_operations", operation_id, legacy_parts=3,
                )
                if len(receipts) > 1:
                    return GuishiQiugouCreateResult("operation_conflict", total_cost=total_cost)
                previous = receipts[0] if receipts else None
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
            if balance is None or as_int_like(balance["stored_stone"]) < total_cost:
                return GuishiQiugouCreateResult("stone_insufficient", total_cost=total_cost)

            uow.execute("SAVEPOINT guishi_create")
            order_id = ""
            for _ in range(20):
                candidate = str(self.order_ids.new_id()).strip()
                if not candidate:
                    continue
                try:
                    inserted = uow.execute(
                        "INSERT INTO guishi_item("
                        "id,user_id,item_id,item_name,item_type,price,quantity) "
                        "VALUES(?,?,?,?,?,xiuxian_int_store(?,'stone','actor'),?)",
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
                    if inserted.rowcount != 1:
                        uow.execute("ROLLBACK TO SAVEPOINT guishi_create")
                        uow.execute("RELEASE SAVEPOINT guishi_create")
                        return GuishiQiugouCreateResult(
                            "state_changed", total_cost=total_cost,
                            reason="guishi_qiugou_order_write_rejected", subject=candidate,
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
                "UPDATE guishi_info SET stored_stone=xiuxian_int_store(xiuxian_int_sub(COALESCE(stored_stone,0),?,'stored_stone','actor')) "
                "WHERE user_id=? AND xiuxian_int_cmp(COALESCE(stored_stone,0),?,1,'stored_stone','actor')>=0",
                (total_cost, user_id, total_cost),
            )
            if charged.rowcount != 1:
                uow.execute("ROLLBACK TO SAVEPOINT guishi_create")
                uow.execute("RELEASE SAVEPOINT guishi_create")
                return GuishiQiugouCreateResult("state_changed", total_cost=total_cost)
            if operation_id:
                receipt = uow.execute(
                    "INSERT INTO guishi_order_create_operations("
                    "operation_id,payload,order_id,order_type,amount,created_at) VALUES(?,?,?,?,xiuxian_int_store(?,'stone','actor'),?)",
                    (operation_id, payload, order_id, "qiugou", total_cost, self.clock.now().isoformat()),
                )
                saved_receipt = uow.query_one(
                    "SELECT payload,order_id,order_type,amount FROM guishi_order_create_operations "
                    "WHERE operation_id=?", (operation_id,)
                )
                if receipt.rowcount != 1 or saved_receipt != {
                    "payload": payload, "order_id": order_id, "order_type": "qiugou", "amount": total_cost,
                }:
                    uow.execute("ROLLBACK TO SAVEPOINT guishi_create")
                    uow.execute("RELEASE SAVEPOINT guishi_create")
                    return GuishiQiugouCreateResult(
                        "state_changed", total_cost=total_cost,
                        reason="guishi_qiugou_receipt_write_rejected", subject=operation_id,
                    )
            saved_order = uow.query_one(
                "SELECT id,user_id,item_id,item_name,item_type,price,quantity,COALESCE(filled_quantity,0) AS filled_quantity "
                "FROM guishi_item WHERE id=?", (order_id,)
            )
            if saved_order != {
                "id": order_id, "user_id": user_id, "item_id": item_id,
                "item_name": item_name, "item_type": "qiugou", "price": price,
                "quantity": quantity, "filled_quantity": 0,
            }:
                uow.execute("ROLLBACK TO SAVEPOINT guishi_create")
                uow.execute("RELEASE SAVEPOINT guishi_create")
                return GuishiQiugouCreateResult(
                    "state_changed", total_cost=total_cost,
                    reason="guishi_qiugou_order_write_rejected", subject=order_id,
                )
            saved_balance = uow.query_one(
                "SELECT stored_stone FROM guishi_info WHERE user_id=?", (user_id,)
            )
            if saved_balance is None or as_int_like(saved_balance["stored_stone"]) != as_int_like(balance["stored_stone"]) - total_cost:
                uow.execute("ROLLBACK TO SAVEPOINT guishi_create")
                uow.execute("RELEASE SAVEPOINT guishi_create")
                return GuishiQiugouCreateResult(
                    "state_changed", total_cost=total_cost,
                    reason="guishi_qiugou_balance_write_rejected", subject=user_id,
                )
            if immediate_match:
                try:
                    enqueue_immediate_match(uow, order_id=order_id, order_type="qiugou", user_id=user_id)
                except DomainError as exc:
                    if exc.code != "guishi_immediate_match_write_rejected":
                        raise
                    uow.execute("ROLLBACK TO SAVEPOINT guishi_create")
                    uow.execute("RELEASE SAVEPOINT guishi_create")
                    return GuishiQiugouCreateResult(
                        "state_changed", total_cost=total_cost,
                        reason=exc.details.get("reason") or exc.code,
                        subject=exc.details.get("subject") or order_id,
                    )
            uow.execute("RELEASE SAVEPOINT guishi_create")
            return GuishiQiugouCreateResult("created", order_id, total_cost)


__all__ = [
    "GuishiOrderIdGenerator",
    "GuishiQiugouCreateResult",
    "GuishiQiugouSqlRepository",
    "OrderIdGenerator",
]
