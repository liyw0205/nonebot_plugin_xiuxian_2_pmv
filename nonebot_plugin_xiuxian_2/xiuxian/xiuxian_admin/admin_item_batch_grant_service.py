from __future__ import annotations

import json
from contextlib import closing
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from threading import RLock

from ..xiuxian_utils import db_backend


@dataclass(frozen=True)
class AdminItemBatchGrantResult:
    status: str
    total: int = 0
    completed: int = 0
    added: int = 0
    granted_users: int = 0

    @property
    def succeeded(self) -> bool:
        return self.status in {"applied", "duplicate"}


class AdminItemBatchGrantService:
    def __init__(self, database: str | Path, lock: RLock | None = None) -> None:
        self._database = Path(database)
        self._lock = lock or RLock()

    @staticmethod
    def _payload(operator_id, user_ids, item_id, item_name, item_type, quantity, max_goods_num):
        return json.dumps(
            {
                "request": [
                    str(operator_id),
                    int(item_id),
                    str(item_name),
                    str(item_type),
                    int(quantity),
                    int(max_goods_num),
                ],
                "users": list(user_ids),
            },
            ensure_ascii=True,
            sort_keys=True,
            separators=(",", ":"),
        )

    @staticmethod
    def _ensure_tables(conn) -> None:
        conn.execute(
            "CREATE TABLE IF NOT EXISTS admin_item_batch_grant_operations ("
            "operation_id TEXT PRIMARY KEY,payload TEXT NOT NULL,total INTEGER NOT NULL,"
            "completed INTEGER NOT NULL DEFAULT 0,added INTEGER NOT NULL DEFAULT 0,"
            "status TEXT NOT NULL DEFAULT 'running',created_at TEXT NOT NULL,updated_at TEXT NOT NULL)"
        )
        conn.execute(
            "CREATE TABLE IF NOT EXISTS admin_item_batch_grant_progress ("
            "operation_id TEXT NOT NULL,user_id TEXT NOT NULL,added INTEGER NOT NULL,"
            "PRIMARY KEY(operation_id,user_id))"
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

    @staticmethod
    def _result(conn, operation_id: str, status: str) -> AdminItemBatchGrantResult:
        row = conn.execute(
            "SELECT total,completed,added FROM admin_item_batch_grant_operations WHERE operation_id=%s",
            (operation_id,),
        ).fetchone()
        granted_users = conn.execute(
            "SELECT COUNT(*) FROM admin_item_batch_grant_progress WHERE operation_id=%s AND added>0",
            (operation_id,),
        ).fetchone()
        return AdminItemBatchGrantResult(
            status,
            int(row[0]),
            int(row[1]),
            int(row[2]),
            int(granted_users[0]),
        )

    def grant(
        self,
        operation_id,
        operator_id,
        user_ids,
        item_id,
        item_name,
        item_type,
        quantity,
        max_goods_num,
        *,
        chunk_size=100,
    ):
        operation_id = str(operation_id).strip()
        operator_id = str(operator_id).strip()
        normalized_users = tuple(sorted({str(user_id) for user_id in user_ids}))
        item_id, quantity, max_goods_num = int(item_id), int(quantity), int(max_goods_num)
        chunk_size = max(1, int(chunk_size))
        if (
            not operation_id
            or not operator_id
            or not normalized_users
            or item_id <= 0
            or quantity <= 0
            or max_goods_num <= 0
        ):
            raise ValueError("invalid batch grant arguments")
        payload = self._payload(
            operator_id,
            normalized_users,
            item_id,
            item_name,
            item_type,
            quantity,
            max_goods_num,
        )
        with self._lock, closing(db_backend.connect(self._database)) as conn:
            try:
                conn.execute("BEGIN IMMEDIATE")
                self._ensure_tables(conn)
                previous = conn.execute(
                    "SELECT payload,total,completed,added,status FROM admin_item_batch_grant_operations WHERE operation_id=%s",
                    (operation_id,),
                ).fetchone()
                now = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
                if previous is None:
                    conn.execute(
                        "INSERT INTO admin_item_batch_grant_operations VALUES (%s,%s,%s,0,0,'running',%s,%s)",
                        (operation_id, payload, len(normalized_users), now, now),
                    )
                else:
                    previous_payload = json.loads(str(previous[0]))
                    current_payload = json.loads(payload)
                    if previous_payload.get("request") != current_payload.get("request"):
                        result = self._result(conn, operation_id, "operation_conflict")
                        conn.rollback()
                        return result
                    normalized_users = tuple(str(user_id) for user_id in previous_payload["users"])
                if previous is not None and str(previous[4]) == "completed":
                    result = self._result(conn, operation_id, "duplicate")
                    conn.rollback()
                    return result
                completed_users = {
                    str(row[0]) for row in conn.execute(
                        "SELECT user_id FROM admin_item_batch_grant_progress WHERE operation_id=%s", (operation_id,)
                    ).fetchall()
                }
                pending = [user_id for user_id in normalized_users if user_id not in completed_users][:chunk_size]
                added_total = 0
                columns = set(conn.column_names("back"))
                for user_id in pending:
                    actual_add = 0
                    if conn.execute("SELECT 1 FROM user_xiuxian WHERE user_id=%s", (user_id,)).fetchone():
                        row = conn.execute(
                            "SELECT COALESCE(goods_num,0) FROM back WHERE user_id=%s AND goods_id=%s", (user_id, item_id)
                        ).fetchone()
                        current = int(row[0]) if row else 0
                        actual_add = quantity if current + quantity <= max_goods_num else 0
                        if actual_add:
                            bind_columns = ",bind_num" if "bind_num" in columns else ""
                            bind_values = ",0" if "bind_num" in columns else ""
                            conn.execute(
                                f"INSERT INTO back (user_id,goods_id,goods_name,goods_type,goods_num,create_time,update_time{bind_columns}) "
                                f"VALUES (%s,%s,%s,%s,%s,%s,%s{bind_values}) ON CONFLICT(user_id,goods_id) DO UPDATE SET "
                                f"goods_name=excluded.goods_name,goods_type=excluded.goods_type,goods_num=back.goods_num+excluded.goods_num,"
                                "update_time=excluded.update_time",
                                (user_id, item_id, str(item_name), str(item_type), actual_add, now, now),
                            )
                            item_delta = json.dumps(
                                [{
                                    "id": item_id,
                                    "name": str(item_name),
                                    "type": str(item_type),
                                    "amount": actual_add,
                                }],
                                ensure_ascii=True,
                                separators=(",", ":"),
                            )
                            detail = json.dumps(
                                {
                                    "operator_id": operator_id,
                                    "requested_quantity": quantity,
                                    "previous_quantity": current,
                                    "final_quantity": current + actual_add,
                                    "target": "all",
                                },
                                ensure_ascii=True,
                                sort_keys=True,
                                separators=(",", ":"),
                            )
                            conn.execute(
                                "INSERT INTO economy_log(user_id,source,action,item_delta,detail,trace_id,created_at) "
                                "VALUES(%s,'admin','admin_item_add_all',%s,%s,%s,%s)",
                                (user_id, item_delta, detail, operation_id, now),
                            )
                    conn.execute("INSERT INTO admin_item_batch_grant_progress VALUES (%s,%s,%s)", (operation_id, user_id, actual_add))
                    added_total += actual_add
                completed = len(completed_users) + len(pending)
                status = "completed" if completed >= len(normalized_users) else "running"
                conn.execute(
                    "UPDATE admin_item_batch_grant_operations SET completed=%s,added=added+%s,status=%s,updated_at=%s WHERE operation_id=%s",
                    (completed, added_total, status, now, operation_id),
                )
                result = self._result(conn, operation_id, "applied")
                conn.commit()
                return result
            except Exception:
                conn.rollback()
                raise


__all__ = ["AdminItemBatchGrantResult", "AdminItemBatchGrantService"]
