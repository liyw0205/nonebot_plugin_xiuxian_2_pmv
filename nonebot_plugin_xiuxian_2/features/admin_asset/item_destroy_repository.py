from __future__ import annotations

import json
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path

from ...infrastructure.database import DatabaseUnitOfWork


@dataclass(frozen=True)
class AdminItemDestroyResult:
    status: str
    previous_quantity: int = 0
    final_quantity: int = 0
    removed_quantity: int = 0

    @property
    def succeeded(self) -> bool:
        return self.status in {"destroyed", "duplicate"}


class AdminItemDestroySqlRepository:
    def __init__(self, database: str | Path) -> None:
        self.database = str(database)

    def destroy(self, operation_id: str, operator_id: str, user_id: str, item_id: int, item_name: str, item_type: str, quantity: int, expected_quantity: int, *, target_name: str = "") -> AdminItemDestroyResult:
        operation_id, operator_id, user_id = str(operation_id).strip(), str(operator_id).strip(), str(user_id).strip()
        item_id, quantity, expected_quantity = int(item_id), int(quantity), int(expected_quantity)
        if not operation_id or not operator_id or not user_id or item_id <= 0 or quantity <= 0 or expected_quantity < 0:
            raise ValueError("valid admin item destroy request is required")
        payload = json.dumps([operator_id, user_id, item_id, str(item_name), str(item_type), quantity, str(target_name)], ensure_ascii=True, separators=(",", ":"))
        with DatabaseUnitOfWork(self.database, immediate=True) as uow:
            uow.execute("CREATE TABLE IF NOT EXISTS admin_item_destroy_operations(operation_id TEXT PRIMARY KEY,payload TEXT NOT NULL,previous_quantity INTEGER NOT NULL,final_quantity INTEGER NOT NULL,removed_quantity INTEGER NOT NULL,created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP)")
            uow.execute("CREATE TABLE IF NOT EXISTS economy_log(id INTEGER PRIMARY KEY AUTOINCREMENT,user_id TEXT,sect_id INTEGER,source TEXT NOT NULL,action TEXT NOT NULL,stone_delta INTEGER NOT NULL DEFAULT 0,exp_delta INTEGER NOT NULL DEFAULT 0,sect_contribution_delta INTEGER NOT NULL DEFAULT 0,sect_scale_delta INTEGER NOT NULL DEFAULT 0,sect_materials_delta INTEGER NOT NULL DEFAULT 0,item_delta TEXT NOT NULL DEFAULT '[]',detail TEXT NOT NULL DEFAULT '{}',trace_id TEXT,created_at TEXT NOT NULL)")
            previous = uow.query_one("SELECT payload,previous_quantity,final_quantity,removed_quantity FROM admin_item_destroy_operations WHERE operation_id=?", (operation_id,))
            if previous is not None:
                if str(previous["payload"]) != payload:
                    return AdminItemDestroyResult("operation_conflict")
                return AdminItemDestroyResult("duplicate", int(previous["previous_quantity"]), int(previous["final_quantity"]), int(previous["removed_quantity"]))
            if uow.query_one("SELECT 1 AS present FROM user_xiuxian WHERE user_id=?", (user_id,)) is None:
                return AdminItemDestroyResult("user_missing")
            columns = {str(row[1]) for row in uow.execute("PRAGMA table_info(back)").fetchall()}
            bind = ",COALESCE(bind_num,0) AS bind_num" if "bind_num" in columns else ""
            row = uow.query_one("SELECT COALESCE(goods_num,0) AS quantity" + bind + " FROM back WHERE user_id=? AND goods_id=?", (user_id, item_id))
            actual = int(row["quantity"]) if row else 0
            actual_bind = int(row["bind_num"]) if row and bind else 0
            if actual != expected_quantity:
                return AdminItemDestroyResult("state_changed", actual, actual)
            if actual <= 0:
                return AdminItemDestroyResult("item_missing")
            removed, final = min(quantity, expected_quantity), expected_quantity - min(quantity, expected_quantity)
            updates = ["goods_num=?", "update_time=?"]
            params = [final, datetime.now()]
            if "bind_num" in columns:
                updates.append("bind_num=?")
                params.append(max(actual_bind - removed, 0) if actual_bind >= removed else min(actual_bind, final))
            params.extend([user_id, item_id, expected_quantity])
            if uow.execute("UPDATE back SET " + ",".join(updates) + " WHERE user_id=? AND goods_id=? AND COALESCE(goods_num,0)=?", tuple(params)).rowcount != 1:
                return AdminItemDestroyResult("state_changed")
            item_delta = json.dumps([{"id": item_id, "name": str(item_name), "type": str(item_type), "amount": -removed}], ensure_ascii=True, separators=(",", ":"))
            detail = json.dumps({"operator_id": operator_id, "target_name": str(target_name), "requested_quantity": quantity, "previous_quantity": expected_quantity, "final_quantity": final}, ensure_ascii=True, sort_keys=True, separators=(",", ":"))
            uow.execute("INSERT INTO economy_log(user_id,source,action,item_delta,detail,trace_id,created_at) VALUES(?,'admin','admin_item_cost',?,?,?,CURRENT_TIMESTAMP)", (user_id, item_delta, detail, operation_id))
            uow.execute("INSERT INTO admin_item_destroy_operations(operation_id,payload,previous_quantity,final_quantity,removed_quantity) VALUES(?,?,?,?,?)", (operation_id, payload, expected_quantity, final, removed))
            return AdminItemDestroyResult("destroyed", expected_quantity, final, removed)


__all__ = ["AdminItemDestroySqlRepository", "AdminItemDestroyResult"]
