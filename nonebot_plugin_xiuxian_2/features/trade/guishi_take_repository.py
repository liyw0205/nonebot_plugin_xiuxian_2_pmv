from __future__ import annotations

import json
from dataclasses import dataclass
from pathlib import Path
from typing import Any

from ...infrastructure.clock import SystemClock
from ...infrastructure.database import DatabaseUnitOfWork


@dataclass(frozen=True)
class GuishiStoredItemTakeResult:
    status: str
    user_id: str
    goods_id: int
    item_name: str = ""
    goods_type: str = ""
    quantity: int = 0

    @property
    def succeeded(self) -> bool:
        return self.status in {"taken", "duplicate"}

    @property
    def applied(self) -> bool:
        return self.status == "taken"


class GuishiStoredItemTakeSqlRepository:
    """Move Guishi storage into bound inventory under one cross-db transaction."""

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
    def _stored_items(value: Any) -> dict[str, int]:
        try:
            items = json.loads(value or "{}")
        except (TypeError, ValueError, json.JSONDecodeError):
            return {}
        if not isinstance(items, dict):
            return {}
        result: dict[str, int] = {}
        for key, quantity in items.items():
            try:
                result[str(key)] = int(quantity)
            except (TypeError, ValueError):
                continue
        return result

    def take(
        self,
        *,
        operation_id: str,
        user_id: str,
        goods_id: int,
        item_name: str,
        goods_type: str,
        max_goods_num: int,
    ) -> GuishiStoredItemTakeResult:
        operation_id = str(operation_id or "").strip()
        user_id = str(user_id).strip()
        goods_id = int(goods_id)
        item_name = str(item_name)
        goods_type = str(goods_type)
        max_goods_num = max(int(max_goods_num), 1)
        if not operation_id:
            raise ValueError("operation_id must not be empty")
        if not user_id or not item_name or not goods_type:
            raise ValueError("user_id, item_name, and goods_type are required")

        with DatabaseUnitOfWork(self.game_database, immediate=True) as uow:
            uow.attach_database(self.trade_database, "guishi_trade")
            previous = uow.query_one(
                "SELECT user_id,goods_id,item_name,goods_type,quantity "
                "FROM guishi_take_item_operations WHERE operation_id=?",
                (operation_id,),
            )
            if previous is not None:
                if (
                    str(previous["user_id"]) != user_id
                    or int(previous["goods_id"]) != goods_id
                    or str(previous["item_name"]) != item_name
                    or str(previous["goods_type"]) != goods_type
                ):
                    return GuishiStoredItemTakeResult("operation_conflict", user_id, goods_id)
                return GuishiStoredItemTakeResult(
                    "duplicate",
                    user_id,
                    goods_id,
                    str(previous["item_name"]),
                    str(previous["goods_type"]),
                    int(previous["quantity"]),
                )

            account = uow.query_one(
                "SELECT items FROM guishi_trade.guishi_info WHERE user_id=?",
                (user_id,),
            )
            if account is None:
                return GuishiStoredItemTakeResult("user_missing", user_id, goods_id)
            stored_items = self._stored_items(account["items"])
            quantity = int(stored_items.get(str(goods_id), 0))
            if quantity <= 0:
                return GuishiStoredItemTakeResult("item_missing", user_id, goods_id)

            inventory = uow.query_one(
                "SELECT COALESCE(goods_num,0) AS goods_num,"
                "COALESCE(bind_num,0) AS bind_num FROM back WHERE user_id=? AND goods_id=?",
                (user_id, goods_id),
            )
            current_quantity = int(inventory["goods_num"]) if inventory else 0
            if current_quantity + quantity > max_goods_num:
                return GuishiStoredItemTakeResult(
                    "inventory_full", user_id, goods_id, item_name, goods_type, quantity
                )

            del stored_items[str(goods_id)]
            uow.execute(
                "UPDATE guishi_trade.guishi_info SET items=? WHERE user_id=?",
                (json.dumps(stored_items, ensure_ascii=False, sort_keys=True), user_id),
            )
            now = self.clock.now().isoformat()
            uow.execute(
                "INSERT INTO back(user_id,goods_id,goods_name,goods_type,goods_num,"
                "create_time,update_time,bind_num) VALUES(?,?,?,?,?,?,?,?) "
                "ON CONFLICT(user_id,goods_id) DO UPDATE SET "
                "goods_name=excluded.goods_name,goods_type=excluded.goods_type,"
                "goods_num=COALESCE(back.goods_num,0)+excluded.goods_num,"
                "bind_num=COALESCE(back.bind_num,0)+excluded.bind_num,"
                "update_time=excluded.update_time",
                (
                    user_id,
                    goods_id,
                    item_name,
                    goods_type,
                    quantity,
                    now,
                    now,
                    quantity,
                ),
            )
            uow.execute(
                "INSERT INTO guishi_take_item_operations(operation_id,user_id,goods_id,"
                "item_name,goods_type,quantity) VALUES(?,?,?,?,?,?)",
                (operation_id, user_id, goods_id, item_name, goods_type, quantity),
            )
            return GuishiStoredItemTakeResult(
                "taken", user_id, goods_id, item_name, goods_type, quantity
            )


__all__ = ["GuishiStoredItemTakeResult", "GuishiStoredItemTakeSqlRepository"]
