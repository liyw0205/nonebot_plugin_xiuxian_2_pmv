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
        self.database = Path(database)

    @staticmethod
    def _schema_ready(uow: DatabaseUnitOfWork) -> bool:
        tables = {
            str(row["name"])
            for row in uow.query_all(
                "SELECT name FROM sqlite_master WHERE type='table' "
                "AND name IN (?,?)",
                ("admin_item_grant_operations", "back"),
            )
        }
        if tables != {"admin_item_grant_operations", "back"}:
            return False

        required_columns = {
            "admin_item_grant_operations": {
                "operation_id", "payload", "user_id", "item_id", "previous_quantity",
                "final_quantity", "granted_quantity",
            },
            "back": {"user_id", "goods_id", "goods_name", "goods_type", "goods_num", "bind_num"},
        }
        for table, required in required_columns.items():
            columns = {
                str(row["name"]).casefold()
                for row in uow.query_all(f'PRAGMA table_info("{table}")')
            }
            if not required.issubset(columns):
                return False
        return True

    @staticmethod
    def _batch_audit_schema_ready(uow: DatabaseUnitOfWork) -> bool:
        required = {
            "economy_log": {"user_id", "source", "action", "item_delta", "detail", "trace_id", "created_at"},
            "user_xiuxian": {"user_id"},
            "back": {"create_time", "update_time"},
        }
        for table, expected in required.items():
            columns = {
                str(row["name"]).casefold()
                for row in uow.query_all(f'PRAGMA table_info("{table}")')
            }
            if not expected.issubset(columns):
                return False
        return True

    def grant(
        self,
        operation_id: str,
        operator_id: str,
        user_id: str,
        item_id: int,
        item_name: str,
        item_type: str,
        quantity: int,
        expected_quantity: int,
        max_goods_num: int,
        *,
        require_user: bool = False,
        audit_action: str | None = None,
        audit_trace_id: str | None = None,
        target_name: str = "",
    ) -> AdminItemResult:
        payload = json.dumps([operator_id, user_id, int(item_id), int(quantity)], separators=(",", ":"))
        if not self.database.is_file():
            return AdminItemResult("schema_missing", user_id, int(item_id))
        with DatabaseUnitOfWork(self.database, immediate=True) as uow:
            if not self._schema_ready(uow) or (
                (require_user or audit_action is not None)
                and not self._batch_audit_schema_ready(uow)
            ):
                return AdminItemResult("schema_missing", user_id, int(item_id))
            if require_user and uow.query_one(
                "SELECT 1 AS present FROM user_xiuxian WHERE user_id=?", (user_id,)
            ) is None:
                return AdminItemResult("user_missing", user_id, int(item_id))
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
                if audit_action is None:
                    uow.execute("INSERT INTO back(user_id,goods_id,goods_name,goods_type,goods_num,bind_num) VALUES(?,?,?,?,?,0)", (user_id, int(item_id), item_name, item_type, int(quantity)))
                else:
                    uow.execute(
                        "INSERT INTO back(user_id,goods_id,goods_name,goods_type,goods_num,"
                        "create_time,update_time,bind_num) VALUES(?,?,?,?,?,CURRENT_TIMESTAMP,CURRENT_TIMESTAMP,0)",
                        (user_id, int(item_id), item_name, item_type, int(quantity)),
                    )
            else:
                if audit_action is None:
                    uow.execute("UPDATE back SET goods_num=goods_num+? WHERE user_id=? AND goods_id=?", (int(quantity), user_id, int(item_id)))
                else:
                    uow.execute(
                        "UPDATE back SET goods_name=?,goods_type=?,goods_num=goods_num+?,"
                        "update_time=CURRENT_TIMESTAMP WHERE user_id=? AND goods_id=?",
                        (item_name, item_type, int(quantity), user_id, int(item_id)),
                    )
            uow.execute("INSERT INTO admin_item_grant_operations(operation_id,payload,user_id,item_id,previous_quantity,final_quantity,granted_quantity) VALUES(?,?,?,?,?,?,?)", (operation_id, payload, user_id, int(item_id), current, final, int(quantity)))
            if audit_action is not None:
                item_delta = json.dumps(
                    [{"id": int(item_id), "name": str(item_name), "type": str(item_type), "amount": int(quantity)}],
                    ensure_ascii=True,
                    separators=(",", ":"),
                )
                detail = json.dumps(
                    {
                        "operator_id": str(operator_id),
                        "requested_quantity": int(quantity),
                        "previous_quantity": current,
                        "final_quantity": final,
                        "target": str(target_name),
                    },
                    ensure_ascii=True,
                    sort_keys=True,
                    separators=(",", ":"),
                )
                uow.execute(
                    "INSERT INTO economy_log(user_id,source,action,item_delta,detail,trace_id,created_at) "
                    "VALUES(?,'admin',?,?,?,?,CURRENT_TIMESTAMP)",
                    (
                        user_id,
                        audit_action,
                        item_delta,
                        detail,
                        str(audit_trace_id or operation_id),
                    ),
                )
            return AdminItemResult("granted", user_id, int(item_id), current, final, int(quantity))


__all__ = ["AdminItemSqlRepository", "AdminItemResult"]
