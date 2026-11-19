from __future__ import annotations

import json
from dataclasses import dataclass
from pathlib import Path
from typing import Any

from ...core.errors import DomainError
from ...core.numeric import bind_sqlite_param, exact_crystal_integer
from ...infrastructure.clock import SystemClock
from ...infrastructure.database import DatabaseUnitOfWork


@dataclass(frozen=True)
class GuishiExpiredOrderClearResult:
    status: str
    order_id: str
    user_id: str = ""
    goods_id: int = 0
    item_name: str = ""
    goods_type: str = ""
    refunded_quantity: int = 0
    reason: str = ""
    subject: str = ""

    @property
    def cleared(self) -> bool:
        return self.status in {"cleared", "duplicate"}

    @property
    def applied(self) -> bool:
        return self.status == "cleared"


class GuishiExpiredOrderSqlRepository:
    """Return unsold Guishi inventory and remove an expired sell order atomically."""

    def __init__(
        self,
        game_database: str | Path,
        trade_database: str | Path,
        *,
        clock: Any | None = None,
    ) -> None:
        self.game_database = str(game_database)
        self.trade_database = str(trade_database)
        self.clock = clock or SystemClock()

    @staticmethod
    def _payload(order_id: str, goods_type: str, expected_user_id: str | None) -> str:
        return json.dumps(
            [order_id, goods_type, str(expected_user_id or "")], ensure_ascii=False
        )

    @staticmethod
    def _from_operation(row: dict[str, Any], status: str) -> GuishiExpiredOrderClearResult:
        for field in ("goods_id", "refunded_quantity"):
            value = exact_crystal_integer(row.get(field))
            if value is None or value < 0:
                return GuishiExpiredOrderClearResult("state_changed", str(row["order_id"]),
                                                     reason="guishi_refund_receipt_invalid", subject=str(row["order_id"]))
        return GuishiExpiredOrderClearResult(
            status=status,
            order_id=str(row["order_id"]),
            user_id=str(row.get("user_id") or ""),
            goods_id=exact_crystal_integer(row.get("goods_id")),
            item_name=str(row.get("item_name") or ""),
            goods_type=str(row.get("goods_type") or ""),
            refunded_quantity=exact_crystal_integer(row.get("refunded_quantity")),
        )

    def clear_baitan(
        self,
        *,
        operation_id: str,
        order_id: str,
        goods_type: str,
        max_goods_num: int,
        expected_user_id: str | None = None,
    ) -> GuishiExpiredOrderClearResult:
        operation_id = str(operation_id or "").strip()
        order_id = str(order_id).strip()
        goods_type = str(goods_type).strip()
        max_goods_num = max(int(max_goods_num), 1)
        expected_user_id = (
            str(expected_user_id).strip() if expected_user_id is not None else None
        )
        if not operation_id:
            raise ValueError("operation_id must not be empty")
        if not order_id:
            raise ValueError("order_id must not be empty")
        if not goods_type:
            raise ValueError("goods_type must not be empty")
        payload = self._payload(order_id, goods_type, expected_user_id)

        with DatabaseUnitOfWork(self.game_database, immediate=True) as uow:
            uow.attach_database(self.trade_database, "guishi_trade")
            previous = uow.query_one(
                "SELECT payload,order_id,user_id,goods_id,item_name,goods_type,"
                "refunded_quantity FROM guishi_trade.guishi_expired_order_operations "
                "WHERE operation_id=?",
                (operation_id,),
            )
            if previous is not None:
                if str(previous["payload"]) != payload:
                    return GuishiExpiredOrderClearResult(
                        "operation_conflict", str(previous["order_id"])
                    )
                return self._from_operation(previous, "duplicate")

            order = uow.query_one(
                "SELECT user_id,item_id,item_name,item_type,quantity,"
                "COALESCE(filled_quantity,0) AS filled_quantity "
                "FROM guishi_trade.guishi_item WHERE id=?",
                (order_id,),
            )
            if order is None:
                return GuishiExpiredOrderClearResult("order_missing", order_id)
            if str(order["item_type"]) not in {"baitan", "摆摊"}:
                return GuishiExpiredOrderClearResult("not_baitan", order_id)

            user_id = str(order["user_id"])
            if expected_user_id is not None and user_id != expected_user_id:
                return GuishiExpiredOrderClearResult("not_owner", order_id, user_id=user_id)

            goods_id, quantity, filled = (exact_crystal_integer(order[field]) for field in ("item_id", "quantity", "filled_quantity"))
            if None in (goods_id, quantity, filled) or goods_id <= 0 or quantity < 0 or not 0 <= filled <= quantity:
                return GuishiExpiredOrderClearResult("state_changed", order_id, reason="guishi_refund_order_invalid", subject=order_id)
            item_name = str(order["item_name"] or "")
            refunded_quantity = quantity - filled
            inventory = uow.query_one(
                "SELECT goods_name,goods_type,COALESCE(goods_num,0) AS goods_num,COALESCE(bind_num,0) AS bind_num FROM back "
                "WHERE user_id=? AND goods_id=?",
                (user_id, goods_id),
            )
            current_quantity = exact_crystal_integer(inventory["goods_num"]) if inventory else 0
            current_bound = exact_crystal_integer(inventory["bind_num"]) if inventory else 0
            if None in (current_quantity, current_bound) or current_quantity < 0 or not 0 <= current_bound <= current_quantity:
                return GuishiExpiredOrderClearResult("state_changed", order_id, reason="inventory_invalid", subject=order_id)
            if inventory is not None:
                inventory.update(goods_num=current_quantity, bind_num=current_bound)
            if current_quantity + refunded_quantity > max_goods_num:
                return GuishiExpiredOrderClearResult(
                    "inventory_full",
                    order_id,
                    user_id,
                    goods_id,
                    item_name,
                    goods_type,
                    refunded_quantity,
                )

            result = GuishiExpiredOrderClearResult(
                "cleared",
                order_id,
                user_id,
                goods_id,
                item_name,
                goods_type,
                refunded_quantity,
            )
            expected_inventory = (dict(goods_name=item_name, goods_type=goods_type,
                                       goods_num=current_quantity + refunded_quantity, bind_num=current_bound)
                                  if refunded_quantity else inventory)
            expected_receipt = dict(payload=payload, order_id=order_id, user_id=user_id, goods_id=goods_id,
                                    item_name=item_name, goods_type=goods_type, refunded_quantity=refunded_quantity)
            try:
                with uow.savepoint("guishi_expired"):
                    if refunded_quantity:
                        now = self.clock.now().isoformat()
                        refunded = uow.execute(
                            "INSERT INTO back(user_id,goods_id,goods_name,goods_type,goods_num,create_time,update_time,bind_num) "
                            "VALUES(?,?,?,?,xiuxian_int_store(?,'goods_num','actor'),?,?,?) ON CONFLICT(user_id,goods_id) DO UPDATE SET "
                            "goods_name=excluded.goods_name,goods_type=excluded.goods_type,"
                            "goods_num=excluded.goods_num,update_time=excluded.update_time",
                            (user_id, goods_id, item_name, goods_type,
                             bind_sqlite_param(current_quantity + refunded_quantity, exact=True), now, now,
                             bind_sqlite_param(current_bound, exact=True)),
                        )
                        if refunded.rowcount != 1:
                            raise DomainError("guishi_refund_inventory_write_rejected", "Inventory refund write rejected")
                    deleted = uow.execute("DELETE FROM guishi_trade.guishi_item WHERE id=?", (order_id,))
                    if deleted.rowcount != 1:
                        raise DomainError("guishi_refund_order_delete_rejected", "Refund order deletion rejected")
                    receipt = uow.execute(
                        "INSERT INTO guishi_trade.guishi_expired_order_operations("
                        "operation_id,payload,order_id,user_id,goods_id,item_name,goods_type,refunded_quantity) VALUES(?,?,?,?,?,?,?,?)",
                        (operation_id, payload, order_id, user_id, goods_id, item_name, goods_type, refunded_quantity),
                    )
                    saved = uow.query_one(
                        "SELECT payload,order_id,user_id,goods_id,item_name,goods_type,refunded_quantity "
                        "FROM guishi_trade.guishi_expired_order_operations WHERE operation_id=?", (operation_id,),
                    )
                    if receipt.rowcount != 1 or saved != expected_receipt:
                        raise DomainError("guishi_refund_receipt_write_rejected", "Refund receipt did not persist exactly")
                    if uow.query_one("SELECT id FROM guishi_trade.guishi_item WHERE id=?", (order_id,)) is not None:
                        raise DomainError("guishi_refund_order_delete_rejected", "Refund order still present")
                    saved = uow.query_one("SELECT goods_name,goods_type,COALESCE(goods_num,0) AS goods_num,"
                                          "COALESCE(bind_num,0) AS bind_num FROM back WHERE user_id=? AND goods_id=?", (user_id, goods_id))
                    if saved is not None:
                        for field in ("goods_num", "bind_num"):
                            saved[field] = exact_crystal_integer(saved[field])
                    if saved != expected_inventory:
                        raise DomainError("guishi_refund_inventory_write_rejected", "Inventory refund did not persist exactly")
            except DomainError as exc:
                return GuishiExpiredOrderClearResult("state_changed", order_id, user_id=user_id,
                                                     reason=exc.details.get("reason") or exc.code,
                                                     subject=exc.details.get("subject") or order_id)
            return result


__all__ = [
    "GuishiExpiredOrderClearResult",
    "GuishiExpiredOrderSqlRepository",
]
