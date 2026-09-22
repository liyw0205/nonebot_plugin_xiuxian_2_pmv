from __future__ import annotations

import json
from pathlib import Path

from ...infrastructure.database import DatabaseUnitOfWork


class AdminItemResult:
    def __init__(self, status: str, user_id: str, item_id: int, previous_quantity: int = 0, final_quantity: int = 0, granted_quantity: int = 0) -> None:
        self.status = status
        self.user_id = user_id
        self.item_id = item_id
        self.previous_quantity = previous_quantity
        self.final_quantity = final_quantity
        self.granted_quantity = granted_quantity


class AdminItemSqlRepository:
    def __init__(self, database: str | Path) -> None:
        self.database = str(database)

    def grant(self, operation_id: str, operator_id: str, user_id: str, item_id: int, item_name: str, item_type: str, quantity: int, expected_quantity: int, max_goods_num: int, **kwargs) -> AdminItemResult:
        payload = json.dumps([operator_id, user_id, int(item_id), int(quantity)], separators=(",", ":"))
        with DatabaseUnitOfWork(self.database, immediate=True) as uow:
            uow.execute("CREATE TABLE IF NOT EXISTS admin_item_grant_operations(operation_id TEXT PRIMARY KEY,payload TEXT NOT NULL,user_id TEXT NOT NULL,item_id INTEGER NOT NULL,previous_quantity INTEGER NOT NULL,final_quantity INTEGER NOT NULL,granted_quantity INTEGER NOT NULL,created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP)")
            previous = uow.query_one("SELECT payload,user_id,item_id,previous_quantity,final_quantity,granted_quantity FROM admin_item_grant_operations WHERE operation_id=?", (operation_id,))
            if previous is not None:
                if str(previous["payload"]) != payload:
                    return AdminItemResult("state_changed", str(previous["user_id"]), int(previous["item_id"]))
                return AdminItemResult("duplicate", str(previous["user_id"]), int(previous["item_id"]), int(previous["previous_quantity"]), int(previous["final_quantity"]), int(previous["granted_quantity"]))
            row = uow.query_one("SELECT COALESCE(goods_num,0) AS goods_num FROM back WHERE user_id=? AND goods_id=?", (user_id, int(item_id)))
            current = int(row["goods_num"]) if row else 0
            if row is not None and current != int(expected_quantity):
                return AdminItemResult("state_changed", user_id, int(item_id), current, current, 0)
            final = current + int(quantity)
            if final > int(max_goods_num):
                return AdminItemResult("inventory_full", user_id, int(item_id), current, current, 0)
            if row is None:
                uow.execute("INSERT INTO back(user_id,goods_id,goods_name,goods_type,goods_num,bind_num) VALUES(?,?,?,?,?,0)", (user_id, int(item_id), item_name, item_type, int(quantity)))
            else:
                uow.execute("UPDATE back SET goods_num=goods_num+? WHERE user_id=? AND goods_id=?", (int(quantity), user_id, int(item_id)))
            uow.execute("INSERT INTO admin_item_grant_operations(operation_id,payload,user_id,item_id,previous_quantity,final_quantity,granted_quantity) VALUES(?,?,?,?,?,?,?)", (operation_id, payload, user_id, int(item_id), current, final, int(quantity)))
            return AdminItemResult("granted", user_id, int(item_id), current, final, int(quantity))


__all__ = ["AdminItemSqlRepository", "AdminItemResult"]
