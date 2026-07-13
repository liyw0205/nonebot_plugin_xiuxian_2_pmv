from __future__ import annotations

import json
from contextlib import closing
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from threading import RLock

from ..xiuxian_utils import db_backend


@dataclass(frozen=True)
class AdminItemGrantResult:
    status: str
    previous_quantity: int = 0
    final_quantity: int = 0
    granted_quantity: int = 0

    @property
    def succeeded(self) -> bool:
        return self.status in {"granted", "duplicate"}


class AdminItemGrantService:
    """Grant one ordinary item and persist its economy audit atomically."""

    def __init__(self, database: str | Path, lock: RLock | None = None) -> None:
        self._database = Path(database)
        self._lock = lock or RLock()

    @staticmethod
    def _ensure_schema(conn) -> None:
        conn.execute(
            "CREATE TABLE IF NOT EXISTS admin_item_grant_operations("
            "operation_id TEXT PRIMARY KEY,payload TEXT NOT NULL,"
            "previous_quantity INTEGER NOT NULL,final_quantity INTEGER NOT NULL,"
            "granted_quantity INTEGER NOT NULL,created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP)"
        )
        conn.execute(
            "CREATE TABLE IF NOT EXISTS economy_log("
            "id INTEGER PRIMARY KEY AUTOINCREMENT,user_id TEXT,sect_id INTEGER,"
            "source TEXT NOT NULL,action TEXT NOT NULL,stone_delta INTEGER NOT NULL DEFAULT 0,"
            "exp_delta INTEGER NOT NULL DEFAULT 0,sect_contribution_delta INTEGER NOT NULL DEFAULT 0,"
            "sect_scale_delta INTEGER NOT NULL DEFAULT 0,sect_materials_delta INTEGER NOT NULL DEFAULT 0,"
            "item_delta TEXT NOT NULL DEFAULT '[]',detail TEXT NOT NULL DEFAULT '{}',"
            "trace_id TEXT,created_at TEXT NOT NULL)"
        )

    def grant(
        self,
        operation_id,
        operator_id,
        user_id,
        item_id,
        item_name,
        item_type,
        quantity,
        expected_quantity,
        max_goods_num,
        *,
        target_name="",
    ) -> AdminItemGrantResult:
        operation_id = str(operation_id).strip()
        operator_id = str(operator_id).strip()
        user_id = str(user_id).strip()
        item_id = int(item_id)
        item_name = str(item_name)
        item_type = str(item_type)
        quantity = int(quantity)
        expected_quantity = int(expected_quantity)
        max_goods_num = int(max_goods_num)
        target_name = str(target_name)
        if not operation_id or not operator_id or not user_id or item_id <= 0:
            raise ValueError("operation, operator, user and item are required")
        if quantity <= 0 or expected_quantity < 0 or max_goods_num <= 0:
            raise ValueError("valid quantity snapshot and inventory limit are required")

        payload = json.dumps(
            [
                operator_id,
                user_id,
                item_id,
                item_name,
                item_type,
                quantity,
                max_goods_num,
                target_name,
            ],
            ensure_ascii=True,
            separators=(",", ":"),
        )
        with self._lock, closing(db_backend.connect(self._database)) as conn:
            try:
                conn.execute("BEGIN IMMEDIATE")
                self._ensure_schema(conn)
                previous = conn.execute(
                    "SELECT payload,previous_quantity,final_quantity,granted_quantity "
                    "FROM admin_item_grant_operations WHERE operation_id=%s",
                    (operation_id,),
                ).fetchone()
                if previous is not None:
                    conn.rollback()
                    if str(previous[0]) != payload:
                        return AdminItemGrantResult("operation_conflict")
                    return AdminItemGrantResult(
                        "duplicate", int(previous[1]), int(previous[2]), int(previous[3])
                    )

                if conn.execute(
                    "SELECT 1 FROM user_xiuxian WHERE user_id=%s", (user_id,)
                ).fetchone() is None:
                    conn.rollback()
                    return AdminItemGrantResult("user_missing")
                row = conn.execute(
                    "SELECT COALESCE(goods_num,0) FROM back WHERE user_id=%s AND goods_id=%s",
                    (user_id, item_id),
                ).fetchone()
                actual_quantity = int(row[0]) if row else 0
                if actual_quantity != expected_quantity:
                    conn.rollback()
                    return AdminItemGrantResult(
                        "state_changed", actual_quantity, actual_quantity
                    )
                final_quantity = expected_quantity + quantity
                if final_quantity > max_goods_num:
                    conn.rollback()
                    return AdminItemGrantResult(
                        "inventory_full", expected_quantity, expected_quantity
                    )

                now = datetime.now()
                columns = set(conn.column_names("back"))
                if row is None:
                    names = [
                        "user_id", "goods_id", "goods_name", "goods_type",
                        "goods_num", "create_time", "update_time",
                    ]
                    values = [user_id, item_id, item_name, item_type, quantity, now, now]
                    if "bind_num" in columns:
                        names.append("bind_num")
                        values.append(0)
                    placeholders = ",".join(["%s"] * len(values))
                    conn.execute(
                        f"INSERT INTO back({','.join(names)}) VALUES({placeholders})",
                        tuple(values),
                    )
                else:
                    changed = conn.execute(
                        "UPDATE back SET goods_name=%s,goods_type=%s,goods_num=%s,update_time=%s "
                        "WHERE user_id=%s AND goods_id=%s AND COALESCE(goods_num,0)=%s",
                        (
                            item_name, item_type, final_quantity, now,
                            user_id, item_id, expected_quantity,
                        ),
                    )
                    if changed.rowcount != 1:
                        conn.rollback()
                        return AdminItemGrantResult("state_changed")

                item_delta = json.dumps(
                    [{"id": item_id, "name": item_name, "type": item_type, "amount": quantity}],
                    ensure_ascii=True,
                    separators=(",", ":"),
                )
                detail = json.dumps(
                    {
                        "operator_id": operator_id,
                        "target_name": target_name,
                        "previous_quantity": expected_quantity,
                        "final_quantity": final_quantity,
                    },
                    ensure_ascii=True,
                    sort_keys=True,
                    separators=(",", ":"),
                )
                conn.execute(
                    "INSERT INTO economy_log(user_id,source,action,item_delta,detail,trace_id,created_at) "
                    "VALUES(%s,'admin','admin_item_add',%s,%s,%s,CURRENT_TIMESTAMP)",
                    (user_id, item_delta, detail, operation_id),
                )
                conn.execute(
                    "INSERT INTO admin_item_grant_operations("
                    "operation_id,payload,previous_quantity,final_quantity,granted_quantity) "
                    "VALUES(%s,%s,%s,%s,%s)",
                    (operation_id, payload, expected_quantity, final_quantity, quantity),
                )
                conn.commit()
                return AdminItemGrantResult(
                    "granted", expected_quantity, final_quantity, quantity
                )
            except Exception:
                conn.rollback()
                raise


__all__ = ["AdminItemGrantResult", "AdminItemGrantService"]
