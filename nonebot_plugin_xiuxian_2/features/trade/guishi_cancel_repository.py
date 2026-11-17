from __future__ import annotations

import json
from dataclasses import dataclass
from pathlib import Path
from typing import Any

from ...core.errors import DomainError
from ...core.numeric import bind_sqlite_param, exact_crystal_integer, require_exact_integer
from ...infrastructure.clock import SystemClock
from ...infrastructure.database import DatabaseUnitOfWork
from .guishi_operation_replay import order_receipts


@dataclass(frozen=True)
class GuishiOrderCancelResult:
    status: str
    order_id: str
    order_type: str = ""
    user_id: str = ""
    goods_id: int = 0
    item_name: str = ""
    goods_type: str = ""
    refunded_quantity: int = 0
    refunded_stone: int = 0
    reason: str = ""
    subject: str = ""

    @property
    def cancelled(self) -> bool:
        return self.status in {"cancelled", "duplicate"}

    @property
    def applied(self) -> bool:
        return self.status == "cancelled"


class GuishiOrderCancelSqlRepository:
    """Cancel Guishi orders with refunds and operation replay in one transaction."""

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
    def _payload(user_id: str, order_id: str, order_type: str) -> str:
        return json.dumps([user_id, order_id, order_type], ensure_ascii=False)

    @staticmethod
    def _from_operation(row: dict[str, Any], status: str) -> GuishiOrderCancelResult:
        for field in ("goods_id", "refunded_quantity", "refunded_stone"):
            value = exact_crystal_integer(row.get(field))
            if value is None or value < 0:
                return GuishiOrderCancelResult("state_changed", str(row["order_id"]),
                                              reason="guishi_refund_receipt_invalid", subject=str(row["order_id"]))
        return GuishiOrderCancelResult(
            status=status,
            order_id=str(row["order_id"]),
            order_type=str(row["order_type"]),
            user_id=str(row["user_id"]),
            goods_id=exact_crystal_integer(row.get("goods_id")),
            item_name=str(row.get("item_name") or ""),
            goods_type=str(row.get("goods_type") or ""),
            refunded_quantity=exact_crystal_integer(row.get("refunded_quantity")),
            refunded_stone=exact_crystal_integer(row.get("refunded_stone")),
        )

    def _replay(
        self,
        uow: DatabaseUnitOfWork,
        operation_id: str,
        payload: str,
        order_type: str,
        *,
        table_name: str,
    ) -> GuishiOrderCancelResult | None:
        if not operation_id:
            return None
        receipts = order_receipts(uow, table_name, operation_id, legacy_parts=1)
        if len(receipts) > 1:
            return GuishiOrderCancelResult("operation_conflict", "", order_type=order_type)
        previous = receipts[0] if receipts else None
        if previous is None:
            return None
        if str(previous["payload"]) != payload or str(previous["order_type"]) != order_type:
            return GuishiOrderCancelResult(
                "operation_conflict", str(previous["order_id"]), order_type=order_type
            )
        return self._from_operation(previous, "duplicate")

    @staticmethod
    def _record(
        uow: DatabaseUnitOfWork,
        *,
        operation_id: str,
        payload: str,
        result: GuishiOrderCancelResult,
        table_name: str,
    ) -> None:
        if not operation_id:
            return
        expected = dict(payload=payload, order_id=result.order_id, order_type=result.order_type,
                        user_id=result.user_id, goods_id=result.goods_id, item_name=result.item_name,
                        goods_type=result.goods_type, refunded_quantity=result.refunded_quantity,
                        refunded_stone=result.refunded_stone)
        written = uow.execute(
            f"INSERT INTO {table_name}("
            "operation_id,payload,order_id,order_type,user_id,goods_id,item_name,goods_type,"
            "refunded_quantity,refunded_stone) VALUES(?,?,?,?,?,?,?,?,?,?)",
            (
                operation_id,
                payload,
                result.order_id,
                result.order_type,
                result.user_id,
                result.goods_id,
                result.item_name,
                result.goods_type,
                result.refunded_quantity,
                bind_sqlite_param(result.refunded_stone, exact=True),
            ),
        )
        saved = uow.query_one(
            f"SELECT {','.join(expected)} FROM {table_name} WHERE operation_id=?", (operation_id,),
        )
        if saved is not None:
            for field in ("goods_id", "refunded_quantity", "refunded_stone"):
                saved[field] = exact_crystal_integer(saved[field])
        if written.rowcount != 1 or saved != expected:
            raise DomainError("guishi_refund_receipt_write_rejected", "Refund receipt did not persist exactly")

    def cancel_qiugou(
        self,
        *,
        operation_id: str,
        user_id: str,
        order_id: str,
    ) -> GuishiOrderCancelResult:
        operation_id = str(operation_id or "").strip()
        user_id = str(user_id).strip()
        order_id = str(order_id).strip()
        payload = self._payload(user_id, order_id, "qiugou")

        with DatabaseUnitOfWork(self.trade_database, immediate=True) as uow:
            replay = self._replay(
                uow,
                operation_id,
                payload,
                "qiugou",
                table_name="guishi_order_cancel_operations",
            )
            if replay is not None:
                return replay
            order = uow.query_one(
                "SELECT user_id,item_id,item_name,item_type,price,quantity,"
                "COALESCE(filled_quantity,0) AS filled_quantity FROM guishi_item WHERE id=?",
                (order_id,),
            )
            if order is None:
                return GuishiOrderCancelResult("order_missing", order_id, order_type="qiugou")
            if str(order["item_type"]) not in {"qiugou", "求购"}:
                return GuishiOrderCancelResult("not_qiugou", order_id, order_type="qiugou")
            owner = str(order["user_id"])
            if owner != user_id:
                return GuishiOrderCancelResult("not_owner", order_id, order_type="qiugou", user_id=owner)

            quantity, filled, price, goods_id = (
                exact_crystal_integer(order[field]) for field in ("quantity", "filled_quantity", "price", "item_id")
            )
            if (None in (quantity, filled, price, goods_id) or quantity < 0 or not 0 <= filled <= quantity
                    or price < 0 or goods_id <= 0):
                return GuishiOrderCancelResult("state_changed", order_id, reason="guishi_refund_order_invalid", subject=order_id)
            refunded_stone = (quantity - filled) * price
            result = GuishiOrderCancelResult(
                "cancelled",
                order_id,
                order_type="qiugou",
                user_id=owner,
                goods_id=goods_id,
                item_name=str(order["item_name"] or ""),
                refunded_stone=refunded_stone,
            )
            try:
                account = uow.query_one("SELECT stored_stone,items FROM guishi_info WHERE user_id=?", (owner,))
                balance = require_exact_integer(account["stored_stone"], field="stored_stone", subject=order_id) if account else 0
                if balance < 0:
                    raise DomainError("guishi_refund_balance_invalid", "Negative stored balance")
                expected_account = dict(stored_stone=balance + refunded_stone, items=account["items"] if account else "{}")
                with uow.savepoint("guishi_cancel"):
                    if refunded_stone:
                        refunded = uow.execute(
                            "INSERT INTO guishi_info(user_id,stored_stone,items) VALUES(?,xiuxian_int_store(?,'stored_stone','actor'),?) "
                            "ON CONFLICT(user_id) DO UPDATE SET stored_stone=excluded.stored_stone",
                            (owner, bind_sqlite_param(balance + refunded_stone, exact=True), expected_account["items"]),
                        )
                        if refunded.rowcount != 1:
                            raise DomainError("guishi_refund_balance_write_rejected", "Refund balance write rejected")
                    deleted = uow.execute("DELETE FROM guishi_item WHERE id=?", (order_id,))
                    if deleted.rowcount != 1:
                        raise DomainError("guishi_refund_order_delete_rejected", "Refund order deletion rejected")
                    self._record(uow, operation_id=operation_id, payload=payload, result=result,
                                 table_name="guishi_order_cancel_operations")
                    if uow.query_one("SELECT id FROM guishi_item WHERE id=?", (order_id,)) is not None:
                        raise DomainError("guishi_refund_order_delete_rejected", "Refund order still present")
                    saved = uow.query_one("SELECT stored_stone,items FROM guishi_info WHERE user_id=?", (owner,))
                    if saved is not None:
                        saved["stored_stone"] = exact_crystal_integer(saved["stored_stone"])
                    if saved != (expected_account if account or refunded_stone else None):
                        raise DomainError("guishi_refund_balance_write_rejected", "Refund balance did not persist exactly")
            except DomainError as exc:
                return GuishiOrderCancelResult("state_changed", order_id, order_type="qiugou", user_id=owner,
                                              reason=exc.details.get("reason") or exc.code,
                                              subject=exc.details.get("subject") or order_id)
            return result

    def cancel_baitan(
        self,
        *,
        operation_id: str,
        user_id: str,
        order_id: str,
        goods_type: str,
        max_goods_num: int,
    ) -> GuishiOrderCancelResult:
        operation_id = str(operation_id or "").strip()
        user_id = str(user_id).strip()
        order_id = str(order_id).strip()
        goods_type = str(goods_type).strip()
        max_goods_num = max(int(max_goods_num), 1)
        if not goods_type:
            raise ValueError("goods_type must not be empty")
        payload = self._payload(user_id, order_id, "baitan")

        with DatabaseUnitOfWork(self.game_database, immediate=True) as uow:
            uow.attach_database(self.trade_database, "guishi_trade")
            replay = self._replay(
                uow,
                operation_id,
                payload,
                "baitan",
                table_name="guishi_trade.guishi_order_cancel_operations",
            )
            if replay is not None:
                return replay
            order = uow.query_one(
                "SELECT user_id,item_id,item_name,item_type,quantity,"
                "COALESCE(filled_quantity,0) AS filled_quantity "
                "FROM guishi_trade.guishi_item WHERE id=?",
                (order_id,),
            )
            if order is None:
                return GuishiOrderCancelResult("order_missing", order_id, order_type="baitan")
            if str(order["item_type"]) not in {"baitan", "摆摊"}:
                return GuishiOrderCancelResult("not_baitan", order_id, order_type="baitan")
            owner = str(order["user_id"])
            if owner != user_id:
                return GuishiOrderCancelResult("not_owner", order_id, order_type="baitan", user_id=owner)

            goods_id, quantity, filled = (exact_crystal_integer(order[field]) for field in ("item_id", "quantity", "filled_quantity"))
            if None in (goods_id, quantity, filled) or goods_id <= 0 or quantity < 0 or not 0 <= filled <= quantity:
                return GuishiOrderCancelResult("state_changed", order_id, reason="guishi_refund_order_invalid", subject=order_id)
            item_name = str(order["item_name"] or "")
            refunded_quantity = quantity - filled
            inventory = uow.query_one(
                "SELECT goods_name,goods_type,COALESCE(goods_num,0) AS goods_num,COALESCE(bind_num,0) AS bind_num "
                "FROM back WHERE user_id=? AND goods_id=?",
                (owner, goods_id),
            )
            current_quantity = exact_crystal_integer(inventory["goods_num"]) if inventory else 0
            current_bound = exact_crystal_integer(inventory["bind_num"]) if inventory else 0
            if None in (current_quantity, current_bound) or current_quantity < 0 or not 0 <= current_bound <= current_quantity:
                return GuishiOrderCancelResult("state_changed", order_id, reason="inventory_invalid", subject=order_id)
            if inventory is not None:
                inventory.update(goods_num=current_quantity, bind_num=current_bound)
            if current_quantity + refunded_quantity > max_goods_num:
                return GuishiOrderCancelResult(
                    "inventory_full",
                    order_id,
                    order_type="baitan",
                    user_id=owner,
                    goods_id=goods_id,
                    item_name=item_name,
                    goods_type=goods_type,
                    refunded_quantity=refunded_quantity,
                )

            result = GuishiOrderCancelResult(
                "cancelled",
                order_id,
                order_type="baitan",
                user_id=owner,
                goods_id=goods_id,
                item_name=item_name,
                goods_type=goods_type,
                refunded_quantity=refunded_quantity,
            )
            expected_inventory = (dict(goods_name=item_name, goods_type=goods_type,
                                       goods_num=current_quantity + refunded_quantity, bind_num=current_bound)
                                  if refunded_quantity else inventory)
            try:
                with uow.savepoint("guishi_cancel"):
                    if refunded_quantity:
                        now = self.clock.now().isoformat()
                        refunded = uow.execute(
                            "INSERT INTO back(user_id,goods_id,goods_name,goods_type,goods_num,create_time,update_time,bind_num) "
                            "VALUES(?,?,?,?,xiuxian_int_store(?,'goods_num','actor'),?,?,?) ON CONFLICT(user_id,goods_id) DO UPDATE SET "
                            "goods_name=excluded.goods_name,goods_type=excluded.goods_type,"
                            "goods_num=excluded.goods_num,update_time=excluded.update_time",
                            (owner, goods_id, item_name, goods_type, bind_sqlite_param(current_quantity + refunded_quantity, exact=True),
                             now, now, bind_sqlite_param(current_bound, exact=True)),
                        )
                        if refunded.rowcount != 1:
                            raise DomainError("guishi_refund_inventory_write_rejected", "Inventory refund write rejected")
                    deleted = uow.execute("DELETE FROM guishi_trade.guishi_item WHERE id=?", (order_id,))
                    if deleted.rowcount != 1:
                        raise DomainError("guishi_refund_order_delete_rejected", "Refund order deletion rejected")
                    self._record(uow, operation_id=operation_id, payload=payload, result=result,
                                 table_name="guishi_trade.guishi_order_cancel_operations")
                    if uow.query_one("SELECT id FROM guishi_trade.guishi_item WHERE id=?", (order_id,)) is not None:
                        raise DomainError("guishi_refund_order_delete_rejected", "Refund order still present")
                    saved = uow.query_one("SELECT goods_name,goods_type,COALESCE(goods_num,0) AS goods_num,"
                                          "COALESCE(bind_num,0) AS bind_num FROM back WHERE user_id=? AND goods_id=?", (owner, goods_id))
                    if saved is not None:
                        for field in ("goods_num", "bind_num"):
                            saved[field] = exact_crystal_integer(saved[field])
                    if saved != expected_inventory:
                        raise DomainError("guishi_refund_inventory_write_rejected", "Inventory refund did not persist exactly")
            except DomainError as exc:
                return GuishiOrderCancelResult("state_changed", order_id, order_type="baitan", user_id=owner,
                                              reason=exc.details.get("reason") or exc.code,
                                              subject=exc.details.get("subject") or order_id)
            return result


__all__ = ["GuishiOrderCancelResult", "GuishiOrderCancelSqlRepository"]
