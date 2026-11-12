from __future__ import annotations

import json
import shutil
from dataclasses import dataclass
from pathlib import Path
from ...core.numeric import require_exact_integer, bind_sqlite_param

from ...infrastructure.database import DatabaseUnitOfWork


@dataclass(frozen=True)
class AdminStoneBatchAdjustmentResult:
    status: str
    total: int = 0
    completed: int = 0
    applied_delta: int = 0
    affected_users: int = 0
    skipped_users: int = 0

    @property
    def succeeded(self) -> bool:
        return self.status in {"applied", "duplicate"}


class AdminStoneBatchSqlRepository:
    reserve_bytes = 8 * 1024 * 1024
    bytes_per_target = 512

    def __init__(self, database: str | Path) -> None:
        self.database = Path(database)

    @staticmethod
    def _request(operator_id: str, requested_delta: int) -> dict[str, object]:
        return {
            "operator_id": str(operator_id).strip(),
            "requested_delta": int(requested_delta),
        }

    @staticmethod
    def _payload(request: dict[str, object]) -> str:
        return json.dumps(request, ensure_ascii=True, sort_keys=True, separators=(",", ":"))

    @staticmethod
    def _schema_ready(uow: DatabaseUnitOfWork) -> bool:
        tables = {
            str(row["name"])
            for row in uow.query_all(
                "SELECT name FROM sqlite_master WHERE type='table' "
                "AND name IN (?,?,?)",
                (
                    "admin_stone_batch_operations",
                    "admin_stone_batch_progress",
                    "user_xiuxian",
                ),
            )
        }
        if tables != {
            "admin_stone_batch_operations",
            "admin_stone_batch_progress",
            "user_xiuxian",
        }:
            return False
        required_columns = {
            "user_xiuxian": {"user_id", "stone"},
            "admin_stone_batch_operations": {
                "operation_id", "operator_id", "requested_delta", "payload", "total",
                "completed", "applied_delta", "affected_users", "skipped_users", "status",
            },
            "admin_stone_batch_progress": {
                "operation_id", "user_id", "status", "previous_stone", "final_stone",
                "applied_delta",
            },
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
    def _result(
        uow: DatabaseUnitOfWork, operation_id: str, status: str
    ) -> AdminStoneBatchAdjustmentResult:
        row = uow.query_one(
            "SELECT total,completed,applied_delta,affected_users,skipped_users "
            "FROM admin_stone_batch_operations WHERE operation_id=?",
            (operation_id,),
        )
        if row is None:
            return AdminStoneBatchAdjustmentResult(status)
        return AdminStoneBatchAdjustmentResult(
            status,
            int(row["total"]),
            int(row["completed"]),
            require_exact_integer(row["applied_delta"], field="stone_delta", subject="batch"),
            int(row["affected_users"]),
            int(row["skipped_users"]),
        )

    def find_running(self, operator_id: str, requested_delta: int) -> str | None:
        if not self.database.is_file():
            return None
        request = self._request(operator_id, requested_delta)
        try:
            with DatabaseUnitOfWork(self.database, read_only=True) as uow:
                if uow.query_one(
                    "SELECT 1 AS present FROM sqlite_master WHERE type='table' "
                    "AND name='admin_stone_batch_operations'"
                ) is None:
                    return None
                row = uow.query_one(
                    "SELECT operation_id FROM admin_stone_batch_operations "
                    "WHERE operator_id=? AND xiuxian_int_cmp(requested_delta,?,1,'stone_amount','actor')=0 AND status='running' "
                    "ORDER BY created_at DESC,rowid DESC LIMIT 1",
                    (request["operator_id"], request["requested_delta"]),
                )
                return str(row["operation_id"]) if row is not None else None
        except Exception:
            return None

    def adjust(
        self,
        operation_id: str,
        operator_id: str,
        requested_delta: int,
        *,
        chunk_size: int = 100,
    ) -> AdminStoneBatchAdjustmentResult:
        operation_id = str(operation_id).strip()
        request = self._request(operator_id, requested_delta)
        chunk_size = max(1, int(chunk_size))
        if not operation_id or not request["operator_id"] or not request["requested_delta"]:
            raise ValueError("invalid stone batch adjustment arguments")
        if not self.database.is_file():
            return AdminStoneBatchAdjustmentResult("not_ready")

        payload = self._payload(request)
        with DatabaseUnitOfWork(self.database, immediate=True) as uow:
            if not self._schema_ready(uow):
                return AdminStoneBatchAdjustmentResult("not_ready")

            previous = uow.query_one(
                "SELECT payload,status,total,completed FROM admin_stone_batch_operations "
                "WHERE operation_id=?",
                (operation_id,),
            )
            if previous is None:
                running = uow.query_one(
                    "SELECT operation_id FROM admin_stone_batch_operations "
                    "WHERE operator_id=? AND xiuxian_int_cmp(requested_delta,?,1,'stone_amount','actor')=0 AND status='running' "
                    "ORDER BY created_at DESC,rowid DESC LIMIT 1",
                    (request["operator_id"], request["requested_delta"]),
                )
                if running is not None:
                    return self._result(uow, str(running["operation_id"]), "in_progress")

                size = uow.query_one(
                    "SELECT COUNT(*) AS total,COUNT(DISTINCT user_id) AS distinct_users,"
                    "SUM(CASE WHEN user_id IS NULL OR TRIM(CAST(user_id AS TEXT))='' "
                    "THEN 1 ELSE 0 END) AS invalid_users FROM user_xiuxian"
                )
                total = int(size["total"])
                if int(size["distinct_users"]) != total or int(size["invalid_users"] or 0):
                    return AdminStoneBatchAdjustmentResult("invalid_schema", total=total)
                required = self.reserve_bytes + total * self.bytes_per_target
                available = shutil.disk_usage(self.database.parent).free
                if available < required:
                    return AdminStoneBatchAdjustmentResult("insufficient_space", total=total)
                status = "completed" if total == 0 else "running"
                uow.execute(
                    "INSERT INTO admin_stone_batch_operations("
                    "operation_id,operator_id,requested_delta,payload,total,status) "
                    "VALUES(?,?,xiuxian_int_store(?,'stone','actor'),?,?,?)",
                    (
                        operation_id,
                        request["operator_id"],
                        request["requested_delta"],
                        payload,
                        total,
                        status,
                    ),
                )
                uow.execute(
                    "INSERT INTO admin_stone_batch_progress("
                    "operation_id,user_id,status,applied_delta) "
                    "SELECT ?,CAST(user_id AS TEXT),'pending',0 FROM user_xiuxian",
                    (operation_id,),
                )
                previous = {
                    "payload": payload,
                    "status": status,
                    "total": total,
                    "completed": 0,
                }
            elif str(previous["payload"]) != payload:
                return self._result(uow, operation_id, "operation_conflict")
            elif str(previous["status"]) == "completed":
                return self._result(uow, operation_id, "duplicate")

            pending = uow.query_all(
                "SELECT user_id FROM admin_stone_batch_progress "
                "WHERE operation_id=? AND status='pending' ORDER BY user_id LIMIT ?",
                (operation_id, chunk_size),
            )
            total = int(previous["total"])
            completed_before = int(previous["completed"])
            if not pending and completed_before < total:
                return self._result(uow, operation_id, "progress_corrupt")

            applied_users = skipped_users = 0
            applied_delta = 0
            delta = int(request["requested_delta"])
            for target in pending:
                user_id = str(target["user_id"])
                row = uow.query_one(
                    "SELECT COALESCE(stone,0) AS stone "
                    "FROM user_xiuxian WHERE user_id=?",
                    (user_id,),
                )
                if row is None:
                    uow.execute(
                        "UPDATE admin_stone_batch_progress SET status='skipped', "
                        "updated_at=CURRENT_TIMESTAMP WHERE operation_id=? AND user_id=?",
                        (operation_id, user_id),
                    )
                    skipped_users += 1
                    continue

                previous_stone = require_exact_integer(row["stone"], field="stone", subject=user_id)
                changed = uow.execute(
                    "UPDATE user_xiuxian SET stone=xiuxian_int_store(xiuxian_int_add(COALESCE(stone,0),?,'stone','actor')) "
                    "WHERE user_id=?",
                    (delta, user_id),
                )
                if changed.rowcount != 1:
                    uow.execute(
                        "UPDATE admin_stone_batch_progress SET status='skipped', "
                        "updated_at=CURRENT_TIMESTAMP WHERE operation_id=? AND user_id=?",
                        (operation_id, user_id),
                    )
                    skipped_users += 1
                    continue
                final_stone = previous_stone + delta
                uow.execute(
                    "UPDATE admin_stone_batch_progress SET status='applied',previous_stone=?,"
                    "final_stone=xiuxian_int_store(?,'stone','actor'),applied_delta=xiuxian_int_store(?,'stone_delta','actor'),updated_at=CURRENT_TIMESTAMP "
                    "WHERE operation_id=? AND user_id=?",
                    (bind_sqlite_param(previous_stone, exact=True), bind_sqlite_param(final_stone, exact=True), bind_sqlite_param(delta, exact=True), operation_id, user_id),
                )
                applied_users += 1
                applied_delta += delta

            completed = completed_before + len(pending)
            status = "completed" if completed >= total else "running"
            uow.execute(
                "UPDATE admin_stone_batch_operations SET completed=?,"
                "applied_delta=xiuxian_int_store(xiuxian_int_add(applied_delta,?,'stone_delta','actor')),affected_users=affected_users+?,"
                "skipped_users=skipped_users+?,status=?,updated_at=CURRENT_TIMESTAMP "
                "WHERE operation_id=?",
                (
                    completed,
                    applied_delta,
                    applied_users,
                    skipped_users,
                    status,
                    operation_id,
                ),
            )
            return self._result(uow, operation_id, "applied")


__all__ = ["AdminStoneBatchAdjustmentResult", "AdminStoneBatchSqlRepository"]
