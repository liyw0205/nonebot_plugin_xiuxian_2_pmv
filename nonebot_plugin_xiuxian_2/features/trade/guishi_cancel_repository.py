from __future__ import annotations

import json
from dataclasses import dataclass
from pathlib import Path
from typing import Any

from ...infrastructure.clock import SystemClock
from ...infrastructure.database import DatabaseUnitOfWork


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
        return GuishiOrderCancelResult(
            status=status,
            order_id=str(row["order_id"]),
            order_type=str(row["order_type"]),
            user_id=str(row["user_id"]),
            goods_id=int(row.get("goods_id") or 0),
            item_name=str(row.get("item_name") or ""),
            goods_type=str(row.get("goods_type") or ""),
            refunded_quantity=int(row.get("refunded_quantity") or 0),
            refunded_stone=int(row.get("refunded_stone") or 0),
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
        previous = uow.query_one(
            "SELECT payload,order_id,order_type,user_id,goods_id,item_name,goods_type,"
            f"refunded_quantity,refunded_stone FROM {table_name} "
            "WHERE operation_id=?",
            (operation_id,),
        )
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
        uow.execute(
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
                result.refunded_stone,
            ),
        )

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

            refunded_stone = max(int(order["quantity"] or 0) - int(order["filled_quantity"] or 0), 0)
            refunded_stone *= max(int(order["price"] or 0), 0)
            if refunded_stone:
                uow.execute(
                    "INSERT INTO guishi_info(user_id,stored_stone,items) VALUES(?,?,?) "
                    "ON CONFLICT(user_id) DO UPDATE SET stored_stone=COALESCE(guishi_info.stored_stone,0)+excluded.stored_stone",
                    (owner, refunded_stone, "{}"),
                )
            uow.execute("DELETE FROM guishi_item WHERE id=?", (order_id,))
            result = GuishiOrderCancelResult(
                "cancelled",
                order_id,
                order_type="qiugou",
                user_id=owner,
                goods_id=int(order["item_id"] or 0),
                item_name=str(order["item_name"] or ""),
                refunded_stone=refunded_stone,
            )
            self._record(
                uow,
                operation_id=operation_id,
                payload=payload,
                result=result,
                table_name="guishi_order_cancel_operations",
            )
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

            goods_id = int(order["item_id"] or 0)
            item_name = str(order["item_name"] or "")
            refunded_quantity = max(int(order["quantity"] or 0) - int(order["filled_quantity"] or 0), 0)
            inventory = uow.query_one(
                "SELECT COALESCE(goods_num,0) AS goods_num FROM back WHERE user_id=? AND goods_id=?",
                (owner, goods_id),
            )
            current_quantity = int(inventory["goods_num"]) if inventory else 0
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

            if refunded_quantity:
                now = self.clock.now().isoformat()
                uow.execute(
                    "INSERT INTO back(user_id,goods_id,goods_name,goods_type,goods_num,create_time,update_time,bind_num) "
                    "VALUES(?,?,?,?,?,?,?,0) ON CONFLICT(user_id,goods_id) DO UPDATE SET "
                    "goods_name=excluded.goods_name,goods_type=excluded.goods_type,"
                    "goods_num=COALESCE(back.goods_num,0)+excluded.goods_num,update_time=excluded.update_time",
                    (owner, goods_id, item_name, goods_type, refunded_quantity, now, now),
                )
            uow.execute("DELETE FROM guishi_trade.guishi_item WHERE id=?", (order_id,))
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
            self._record(
                uow,
                operation_id=operation_id,
                payload=payload,
                result=result,
                table_name="guishi_trade.guishi_order_cancel_operations",
            )
            return result


__all__ = ["GuishiOrderCancelResult", "GuishiOrderCancelSqlRepository"]
