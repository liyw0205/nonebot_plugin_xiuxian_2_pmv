from __future__ import annotations

import hashlib
import json
from contextlib import closing
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from threading import RLock

from ..xiuxian_utils import db_backend


@dataclass(frozen=True)
class BackpackRepairResult:
    status: str
    operation_id: str
    total: int = 0
    completed: int = 0
    quantity_fixed: int = 0
    bind_fixed: int = 0
    name_fixed: int = 0
    equipment_fixed: int = 0
    missing_definitions: int = 0
    details: tuple[dict, ...] = ()

    @property
    def succeeded(self) -> bool:
        return self.status in {"applied", "duplicate"}

    @property
    def done(self) -> bool:
        return self.succeeded and self.completed >= self.total


class BackpackRepairService:
    """Repair all backpack rows through a resumable database task."""

    _TASK_COLUMNS = (
        "operation_id",
        "payload",
        "catalog_json",
        "max_goods_num",
        "targets_json",
        "next_index",
        "total",
        "quantity_fixed",
        "bind_fixed",
        "name_fixed",
        "equipment_fixed",
        "missing_definitions",
        "details_json",
        "status",
    )

    def __init__(self, database: str | Path, lock: RLock | None = None) -> None:
        self._database = Path(database)
        self._lock = lock or RLock()

    @staticmethod
    def _ensure_schema(conn) -> None:
        conn.execute(
            "CREATE TABLE IF NOT EXISTS backpack_repair_tasks("
            "operation_id TEXT PRIMARY KEY,payload TEXT NOT NULL,"
            "catalog_json TEXT NOT NULL,max_goods_num INTEGER NOT NULL,"
            "targets_json TEXT NOT NULL,"
            "next_index INTEGER NOT NULL DEFAULT 0,total INTEGER NOT NULL,"
            "quantity_fixed INTEGER NOT NULL DEFAULT 0,"
            "bind_fixed INTEGER NOT NULL DEFAULT 0,"
            "name_fixed INTEGER NOT NULL DEFAULT 0,"
            "equipment_fixed INTEGER NOT NULL DEFAULT 0,"
            "missing_definitions INTEGER NOT NULL DEFAULT 0,"
            "details_json TEXT NOT NULL DEFAULT '[]',"
            "status TEXT NOT NULL DEFAULT 'running',"
            "created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,"
            "updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP)"
        )

    @staticmethod
    def _json(value) -> str:
        return json.dumps(
            value, ensure_ascii=False, sort_keys=True, separators=(",", ":")
        )

    @classmethod
    def _normalize_catalog(cls, catalog) -> dict[str, str]:
        if not isinstance(catalog, dict):
            raise ValueError("item catalog must be a mapping")
        return {
            str(item_id): str(item_name)
            for item_id, item_name in catalog.items()
            if str(item_id).strip()
            and item_name is not None
            and str(item_name).strip()
        }

    @classmethod
    def _payload(cls, catalog: dict[str, str], max_goods_num: int) -> str:
        request = cls._json(
            {"catalog": catalog, "max_goods_num": int(max_goods_num)}
        )
        return hashlib.sha256(request.encode("utf-8")).hexdigest()

    @classmethod
    def _fetch_task(cls, conn, operation_id: str):
        row = conn.execute(
            "SELECT " + ",".join(cls._TASK_COLUMNS) +
            " FROM backpack_repair_tasks WHERE operation_id=%s",
            (operation_id,),
        ).fetchone()
        if row is None:
            return None
        return dict(zip(cls._TASK_COLUMNS, row))

    @staticmethod
    def _collect_targets(conn) -> list[dict[str, str]]:
        targets = {}
        if conn.table_exists("user_xiuxian"):
            for user_id, user_name in conn.execute(
                "SELECT user_id,user_name FROM user_xiuxian ORDER BY user_id"
            ).fetchall():
                targets[str(user_id)] = str(user_name or user_id)
        if conn.table_exists("back"):
            for row in conn.execute(
                "SELECT DISTINCT user_id FROM back ORDER BY user_id"
            ).fetchall():
                user_id = str(row[0])
                targets.setdefault(user_id, user_id)
        return [
            {"user_id": user_id, "user_name": targets[user_id]}
            for user_id in sorted(targets)
        ]

    @classmethod
    def _result(cls, task, status: str | None = None) -> BackpackRepairResult:
        details = json.loads(str(task["details_json"]) or "[]")
        return BackpackRepairResult(
            status or str(task["status"]),
            str(task["operation_id"]),
            int(task["total"]),
            int(task["next_index"]),
            int(task["quantity_fixed"]),
            int(task["bind_fixed"]),
            int(task["name_fixed"]),
            int(task["equipment_fixed"]),
            int(task["missing_definitions"]),
            tuple(details),
        )

    @staticmethod
    def _append_detail(details: list[dict], detail: dict) -> None:
        if len(details) < 20:
            details.append(detail)

    @staticmethod
    def _as_int(value) -> int:
        try:
            return int(value or 0)
        except (TypeError, ValueError):
            return 0

    def _repair_equipment(
        self,
        conn,
        user_id: str,
        user_name: str,
        item_id: int,
        item_type: str,
        catalog: dict[str, str],
        back_columns: set[str],
        details: list[dict],
    ) -> tuple[int, int]:
        item_name = catalog.get(str(item_id))
        if not item_name:
            self._append_detail(
                details,
                {
                    "kind": "missing_definition",
                    "user_id": user_id,
                    "user_name": user_name,
                    "item_id": item_id,
                    "item_type": item_type,
                },
            )
            return 0, 1

        row = conn.execute(
            "SELECT goods_num,bind_num,state,goods_name FROM back "
            "WHERE user_id=%s AND goods_id=%s",
            (user_id, item_id),
        ).fetchone()
        now = datetime.now()
        fixes = []
        if row is None:
            values = {
                "user_id": user_id,
                "goods_id": item_id,
                "goods_name": item_name,
                "goods_type": "装备",
                "goods_num": 1,
                "bind_num": 1,
                "state": 1,
                "create_time": now,
                "update_time": now,
                "action_time": now,
            }
            names = [name for name in values if name in back_columns]
            conn.execute(
                "INSERT INTO back(" + ",".join(names) + ") VALUES(" +
                ",".join(["%s"] * len(names)) + ")",
                tuple(values[name] for name in names),
            )
            fixes.extend(("quantity", "state", "name"))
        else:
            goods_num = self._as_int(row[0])
            bind_num = self._as_int(row[1])
            state = self._as_int(row[2])
            old_name = str(row[3] or "")
            updates = []
            params = []
            if goods_num <= 0:
                updates.append("goods_num=%s")
                params.append(1)
                if "bind_num" in back_columns:
                    updates.append("bind_num=%s")
                    params.append(1)
                fixes.append("quantity")
            elif bind_num > goods_num and "bind_num" in back_columns:
                updates.append("bind_num=%s")
                params.append(goods_num)
            if state != 1:
                updates.append("state=%s")
                params.append(1)
                fixes.append("state")
            if old_name != item_name:
                updates.append("goods_name=%s")
                params.append(item_name)
                fixes.append("name")
            if updates:
                if "update_time" in back_columns:
                    updates.append("update_time=%s")
                    params.append(now)
                if "action_time" in back_columns:
                    updates.append("action_time=%s")
                    params.append(now)
                params.extend((user_id, item_id))
                conn.execute(
                    "UPDATE back SET " + ",".join(updates) +
                    " WHERE user_id=%s AND goods_id=%s",
                    tuple(params),
                )

        if fixes:
            self._append_detail(
                details,
                {
                    "kind": "equipment",
                    "user_id": user_id,
                    "user_name": user_name,
                    "item_id": item_id,
                    "item_name": item_name,
                    "item_type": item_type,
                    "fixes": fixes,
                },
            )
            return 1, 0
        return 0, 0

    def _repair_user(
        self,
        conn,
        target: dict,
        catalog: dict[str, str],
        max_goods_num: int,
        details: list[dict],
    ) -> tuple[int, int, int, int, int]:
        user_id = str(target["user_id"])
        user_name = str(target["user_name"])
        quantity_fixed = bind_fixed = name_fixed = 0
        equipment_fixed = missing_definitions = 0
        back_columns = set(conn.column_names("back"))
        rows = conn.execute(
            "SELECT goods_id,goods_num,bind_num,goods_name FROM back "
            "WHERE user_id=%s ORDER BY goods_id",
            (user_id,),
        ).fetchall()
        for item_id, goods_num_raw, bind_num_raw, goods_name_raw in rows:
            item_id = int(item_id)
            goods_num = self._as_int(goods_num_raw)
            bind_num = self._as_int(bind_num_raw)
            goods_name = str(goods_name_raw or "")
            new_goods_num = min(max(goods_num, 0), max_goods_num)
            new_bind_num = min(max(bind_num, 0), new_goods_num)
            new_name = catalog.get(str(item_id), goods_name)
            updates = []
            params = []
            if new_goods_num != goods_num:
                updates.append("goods_num=%s")
                params.append(new_goods_num)
                quantity_fixed += 1
                self._append_detail(
                    details,
                    {
                        "kind": "quantity",
                        "user_id": user_id,
                        "item_id": item_id,
                        "before": goods_num,
                        "after": new_goods_num,
                    },
                )
            if new_bind_num != bind_num:
                updates.append("bind_num=%s")
                params.append(new_bind_num)
                bind_fixed += 1
                self._append_detail(
                    details,
                    {
                        "kind": "bind_quantity",
                        "user_id": user_id,
                        "item_id": item_id,
                        "before": bind_num,
                        "after": new_bind_num,
                    },
                )
            if new_name != goods_name:
                updates.append("goods_name=%s")
                params.append(new_name)
                name_fixed += 1
                self._append_detail(
                    details,
                    {
                        "kind": "name",
                        "user_id": user_id,
                        "item_id": item_id,
                        "before": goods_name,
                        "after": new_name,
                    },
                )
            if updates:
                if "update_time" in back_columns:
                    updates.append("update_time=%s")
                    params.append(datetime.now())
                params.extend((user_id, item_id))
                conn.execute(
                    "UPDATE back SET " + ",".join(updates) +
                    " WHERE user_id=%s AND goods_id=%s",
                    tuple(params),
                )

        if conn.table_exists("BuffInfo"):
            buff_columns = set(conn.column_names("BuffInfo"))
            selected = [
                column
                for column in ("faqi_buff", "armor_buff")
                if column in buff_columns
            ]
            if selected:
                buff_row = conn.execute(
                    "SELECT " + ",".join(selected) +
                    " FROM BuffInfo WHERE user_id=%s",
                    (user_id,),
                ).fetchone()
                if buff_row:
                    item_types = {"faqi_buff": "法器", "armor_buff": "防具"}
                    seen = set()
                    for column, value in zip(selected, buff_row):
                        item_id = self._as_int(value)
                        if item_id <= 0 or item_id in seen:
                            continue
                        seen.add(item_id)
                        fixed, missing = self._repair_equipment(
                            conn,
                            user_id,
                            user_name,
                            item_id,
                            item_types[column],
                            catalog,
                            back_columns,
                            details,
                        )
                        equipment_fixed += fixed
                        missing_definitions += missing

        return (
            quantity_fixed,
            bind_fixed,
            name_fixed,
            equipment_fixed,
            missing_definitions,
        )

    def run(
        self,
        operation_id,
        catalog=None,
        max_goods_num=None,
        *,
        batch_size=100,
    ) -> BackpackRepairResult:
        operation_id = str(operation_id).strip()
        batch_size = int(batch_size)
        if not operation_id or batch_size <= 0:
            raise ValueError("operation id and positive batch size are required")
        normalized_catalog = None
        request_payload = None
        if catalog is not None or max_goods_num is not None:
            if catalog is None or max_goods_num is None or int(max_goods_num) <= 0:
                raise ValueError("catalog and inventory limit must be provided together")
            normalized_catalog = self._normalize_catalog(catalog)
            max_goods_num = int(max_goods_num)
            request_payload = self._payload(normalized_catalog, max_goods_num)

        with self._lock, closing(db_backend.connect(self._database)) as conn:
            try:
                conn.execute("BEGIN IMMEDIATE")
                self._ensure_schema(conn)
                created = False
                resumed_active = False
                task = self._fetch_task(conn, operation_id)
                if task is None:
                    active = conn.execute(
                        "SELECT operation_id FROM backpack_repair_tasks "
                        "WHERE status='running' ORDER BY created_at LIMIT 1"
                    ).fetchone()
                    if active is not None:
                        operation_id = str(active[0])
                        task = self._fetch_task(conn, operation_id)
                        resumed_active = True
                    else:
                        if normalized_catalog is None or request_payload is None:
                            conn.rollback()
                            return BackpackRepairResult("task_missing", operation_id)
                        targets = self._collect_targets(conn)
                        conn.execute(
                            "INSERT INTO backpack_repair_tasks("
                            "operation_id,payload,catalog_json,max_goods_num,"
                            "targets_json,total,status) VALUES(%s,%s,%s,%s,%s,%s,%s)",
                            (
                                operation_id,
                                request_payload,
                                self._json(normalized_catalog),
                                max_goods_num,
                                self._json(targets),
                                len(targets),
                                "running" if targets else "completed",
                            ),
                        )
                        task = self._fetch_task(conn, operation_id)
                        created = True

                if (
                    request_payload is not None
                    and not resumed_active
                    and str(task["payload"]) != request_payload
                ):
                    conn.rollback()
                    return BackpackRepairResult("operation_conflict", operation_id)
                if str(task["status"]) == "completed":
                    if created:
                        conn.commit()
                        return self._result(task, "applied")
                    conn.rollback()
                    return self._result(task, "duplicate")

                catalog_snapshot = json.loads(str(task["catalog_json"]))
                max_goods_num = int(task["max_goods_num"])

                targets = json.loads(str(task["targets_json"]))
                details = json.loads(str(task["details_json"]) or "[]")
                start = int(task["next_index"])
                end = min(start + batch_size, int(task["total"]))
                counters = [
                    int(task["quantity_fixed"]),
                    int(task["bind_fixed"]),
                    int(task["name_fixed"]),
                    int(task["equipment_fixed"]),
                    int(task["missing_definitions"]),
                ]
                for target in targets[start:end]:
                    changes = self._repair_user(
                        conn,
                        target,
                        catalog_snapshot,
                        max_goods_num,
                        details,
                    )
                    counters = [left + right for left, right in zip(counters, changes)]

                status = "completed" if end >= int(task["total"]) else "running"
                conn.execute(
                    "UPDATE backpack_repair_tasks SET next_index=%s,"
                    "quantity_fixed=%s,bind_fixed=%s,name_fixed=%s,"
                    "equipment_fixed=%s,missing_definitions=%s,details_json=%s,"
                    "status=%s,updated_at=CURRENT_TIMESTAMP WHERE operation_id=%s",
                    (
                        end,
                        *counters,
                        self._json(details),
                        status,
                        operation_id,
                    ),
                )
                conn.commit()
                return self._result(self._fetch_task(conn, operation_id), "applied")
            except Exception:
                conn.rollback()
                raise


__all__ = ["BackpackRepairResult", "BackpackRepairService"]
