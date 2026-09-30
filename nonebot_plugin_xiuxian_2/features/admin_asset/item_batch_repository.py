from __future__ import annotations

import json
import shutil
from collections.abc import Iterable
from dataclasses import dataclass
from pathlib import Path
from typing import Any

from ...infrastructure.database import DatabaseUnitOfWork
from .item_destroy_repository import AdminItemDestroyResult, AdminItemDestroySqlRepository
from .item_repository import AdminItemResult, AdminItemSqlRepository


@dataclass(frozen=True)
class AdminItemBatchAdjustmentResult:
    status: str
    action: str
    total: int = 0
    completed: int = 0
    added: int = 0
    removed: int = 0
    affected_users: int = 0
    skipped_users: int = 0

    @property
    def granted_users(self) -> int:
        return self.affected_users if self.action == "grant" else 0

    @property
    def succeeded(self) -> bool:
        return self.status in {"applied", "duplicate"}


class AdminItemBatchSqlRepository:
    """Freeze item batch rosters in game DB and advance them in bounded chunks."""

    reserve_bytes = 8 * 1024 * 1024
    bytes_per_target = 2048
    target_insert_chunk_size = 500
    max_chunk_size = 100
    payload_prefix_chars = 4096
    max_legacy_payload_chars = 64 * 1024 * 1024

    def __init__(
        self,
        database: str | Path,
        item_repository: AdminItemSqlRepository | None = None,
        destroy_repository: AdminItemDestroySqlRepository | None = None,
    ) -> None:
        self.database = Path(database)
        self.item_repository = item_repository or AdminItemSqlRepository(database)
        self.destroy_repository = destroy_repository or AdminItemDestroySqlRepository(database)

    @staticmethod
    def _request(
        action: str,
        operator_id: str,
        item_id: int,
        item_name: str,
        item_type: str,
        quantity: int,
        max_goods_num: int = 0,
    ) -> list[Any]:
        request: list[Any] = [
            str(operator_id).strip(),
            int(item_id),
            str(item_name),
            str(item_type),
            int(quantity),
        ]
        if action == "grant":
            request.append(int(max_goods_num))
        return request

    @staticmethod
    def _payload(request: list[Any]) -> str:
        return json.dumps(
            {"request": request},
            ensure_ascii=True,
            sort_keys=True,
            separators=(",", ":"),
        )

    @classmethod
    def _legacy_payload_prefix(cls, request: list[Any]) -> str:
        return cls._payload(request)[:-1] + ',"users":['

    @staticmethod
    def _payload_request(payload_prefix: str) -> tuple[Any, bool]:
        marker = '{"request":'
        if not payload_prefix.startswith(marker):
            return None, False
        try:
            request, end = json.JSONDecoder().raw_decode(payload_prefix, len(marker))
        except (TypeError, ValueError, json.JSONDecodeError):
            return None, False
        return request, payload_prefix[end:].startswith(',"users":[')

    @classmethod
    def _matches_request(cls, payload_prefix: str, request: list[Any]) -> bool:
        saved, _ = cls._payload_request(payload_prefix)
        return saved == request

    @staticmethod
    def _columns(uow: DatabaseUnitOfWork, table: str) -> set[str]:
        return {
            str(row["name"]).casefold()
            for row in uow.query_all(f'PRAGMA table_info("{table}")')
        }

    @classmethod
    def _schema_ready(cls, uow: DatabaseUnitOfWork, action: str) -> bool:
        required = {
            "user_xiuxian": {"user_id"},
            "back": {"user_id", "goods_id", "goods_name", "goods_type", "goods_num", "bind_num"},
            "economy_log": {"user_id", "source", "action", "item_delta", "detail", "trace_id", "created_at"},
            "admin_item_batch_grant_operations": {
                "operation_id", "payload", "total", "completed", "added", "status",
            },
            "admin_item_batch_grant_progress": {"operation_id", "user_id", "added"},
            "admin_item_batch_operations": {
                "operation_id", "action", "payload", "total", "status", "created_at", "updated_at",
            },
            "admin_item_batch_targets": {
                "operation_id", "user_id", "status", "added_quantity", "removed_quantity", "result_json",
            },
        }
        if action == "grant":
            required["back"].update({"create_time", "update_time"})
            required["admin_item_grant_operations"] = {
                "operation_id", "payload", "user_id", "item_id", "previous_quantity",
                "final_quantity", "granted_quantity",
            }
        else:
            required["back"].update({"update_time"})
            required["admin_item_destroy_operations"] = {
                "operation_id", "payload", "previous_quantity", "final_quantity", "removed_quantity",
            }
        for table, expected in required.items():
            if not expected.issubset(cls._columns(uow, table)):
                return False
        return True

    def _has_space(self, target_count: int) -> bool:
        try:
            free = shutil.disk_usage(self.database.parent).free
        except OSError:
            return False
        return free >= self.reserve_bytes + max(0, target_count) * self.bytes_per_target

    @classmethod
    def _result(
        cls,
        uow: DatabaseUnitOfWork,
        operation_id: str,
        status: str,
        action: str,
    ) -> AdminItemBatchAdjustmentResult:
        operation = uow.query_one(
            "SELECT action,total FROM admin_item_batch_operations WHERE operation_id=?",
            (operation_id,),
        )
        if operation is None:
            return AdminItemBatchAdjustmentResult(status, action)
        targets = uow.query_one(
            "SELECT COUNT(*) AS total FROM admin_item_batch_targets WHERE operation_id=?",
            (operation_id,),
        )
        counts = uow.query_one(
            "SELECT SUM(CASE WHEN status!='pending' THEN 1 ELSE 0 END) AS completed,"
            "COALESCE(SUM(added_quantity),0) AS added,"
            "COALESCE(SUM(removed_quantity),0) AS removed,"
            "SUM(CASE WHEN status!='pending' AND "
            "CASE WHEN ?='grant' THEN added_quantity ELSE removed_quantity END>0 THEN 1 ELSE 0 END) "
            "AS affected_users FROM admin_item_batch_targets WHERE operation_id=?",
            (str(operation["action"]), operation_id),
        )
        completed = int(counts["completed"] or 0)
        affected = int(counts["affected_users"] or 0)
        if int(targets["total"]) == 0:
            legacy = uow.query_one(
                "SELECT payload,total,completed,added,status FROM admin_item_batch_grant_operations "
                "WHERE operation_id=?",
                (operation_id,),
            )
            if legacy is not None:
                return cls._legacy_result_row(legacy, completed=completed, status=status)
        return AdminItemBatchAdjustmentResult(
            status,
            str(operation["action"]),
            int(operation["total"]),
            completed,
            int(counts["added"] or 0),
            int(counts["removed"] or 0),
            affected,
            completed - affected,
        )

    @staticmethod
    def _legacy_result_row(
        row: Any,
        *,
        completed: int | None = None,
        status: str,
    ) -> AdminItemBatchAdjustmentResult:
        return AdminItemBatchAdjustmentResult(
            status,
            "grant",
            int(row["total"]),
            int(row["completed"] if completed is None else completed),
            int(row["added"]),
            0,
            0,
            0,
        )

    def _legacy_result(
        self, uow: DatabaseUnitOfWork, operation_id: str, status: str
    ) -> AdminItemBatchAdjustmentResult:
        row = uow.query_one(
            "SELECT total,completed,added FROM admin_item_batch_grant_operations "
            "WHERE operation_id=?",
            (operation_id,),
        )
        if row is None:
            return AdminItemBatchAdjustmentResult(status, "grant")
        progress = uow.query_one(
            "SELECT COUNT(*) AS granted FROM admin_item_batch_grant_progress "
            "WHERE operation_id=? AND added>0",
            (operation_id,),
        )
        completed = int(row["completed"])
        granted = int(progress["granted"] or 0)
        return AdminItemBatchAdjustmentResult(
            status,
            "grant",
            int(row["total"]),
            completed,
            int(row["added"]),
            0,
            granted,
            completed - granted,
        )

    def find_running(
        self,
        action: str,
        operator_id: str,
        item_id: int,
        item_name: str,
        item_type: str,
        quantity: int,
        max_goods_num: int = 0,
    ) -> str | None:
        if not self.database.is_file():
            return None
        request = self._request(
            action, operator_id, item_id, item_name, item_type, quantity, max_goods_num
        )
        try:
            with DatabaseUnitOfWork(self.database, read_only=True) as uow:
                row = uow.query_one(
                    "SELECT operation_id FROM admin_item_batch_operations "
                    "WHERE action=? AND status='running' AND payload=? "
                    "ORDER BY created_at DESC,rowid DESC LIMIT 1",
                    (action, self._payload(request)),
                )
                if row is not None:
                    return str(row["operation_id"])
                if action == "grant":
                    row = uow.query_one(
                        "SELECT operation_id FROM admin_item_batch_grant_operations "
                        "WHERE status='running' AND substr(payload,1,?)=? "
                        "ORDER BY created_at DESC,rowid DESC LIMIT 1",
                        (
                            len(self._legacy_payload_prefix(request)),
                            self._legacy_payload_prefix(request),
                        ),
                    )
                    if row is not None:
                        return str(row["operation_id"])
        except Exception:
            return None
        return None

    @staticmethod
    def _insert_targets(
        uow: DatabaseUnitOfWork,
        operation_id: str,
        user_ids: Iterable[Any],
        chunk_size: int,
    ) -> None:
        batch: list[tuple[str, str]] = []
        for raw_user_id in user_ids:
            user_id = str(raw_user_id).strip()
            if not user_id:
                continue
            batch.append((operation_id, user_id))
            if len(batch) >= chunk_size:
                uow.executemany(
                    "INSERT OR IGNORE INTO admin_item_batch_targets"
                    "(operation_id,user_id,status,added_quantity,removed_quantity,result_json) "
                    "VALUES(?,?,'pending',0,0,'{}')",
                    batch,
                )
                batch.clear()
        if batch:
            uow.executemany(
                "INSERT OR IGNORE INTO admin_item_batch_targets"
                "(operation_id,user_id,status,added_quantity,removed_quantity,result_json) "
                "VALUES(?,?,'pending',0,0,'{}')",
                batch,
            )

    @staticmethod
    def _iter_legacy_users(payload: str) -> Iterable[Any]:
        request_marker = '{"request":'
        decoder = json.JSONDecoder()
        try:
            _, index = decoder.raw_decode(payload, len(request_marker))
        except (TypeError, ValueError, json.JSONDecodeError) as exc:
            raise ValueError("invalid legacy item batch payload") from exc
        users_marker = ',"users":['
        if not payload.startswith(users_marker, index):
            raise ValueError("legacy item batch has no frozen users")
        index += len(users_marker)
        while True:
            while index < len(payload) and payload[index].isspace():
                index += 1
            if index >= len(payload):
                raise ValueError("truncated legacy item batch users")
            if payload[index] == "]":
                return
            try:
                user_id, index = decoder.raw_decode(payload, index)
            except (TypeError, ValueError, json.JSONDecodeError) as exc:
                raise ValueError("invalid legacy item batch user") from exc
            yield user_id
            while index < len(payload) and payload[index].isspace():
                index += 1
            if index >= len(payload):
                raise ValueError("truncated legacy item batch users")
            if payload[index] == ",":
                index += 1
            elif payload[index] != "]":
                raise ValueError("invalid legacy item batch user separator")
            else:
                return

    @staticmethod
    def _import_legacy_progress(uow: DatabaseUnitOfWork, operation_id: str) -> None:
        uow.execute(
            "UPDATE admin_item_batch_targets AS t SET status='legacy_completed',"
            "added_quantity=COALESCE((SELECT p.added FROM admin_item_batch_grant_progress p "
            "WHERE p.operation_id=t.operation_id AND p.user_id=t.user_id),0),result_json='{}' "
            "WHERE t.operation_id=? AND t.status='pending' AND EXISTS ("
            "SELECT 1 FROM admin_item_batch_grant_progress p "
            "WHERE p.operation_id=t.operation_id AND p.user_id=t.user_id)",
            (operation_id,),
        )

    def _begin(
        self,
        operation_id: str,
        action: str,
        request: list[Any],
        chunk_size: int,
    ) -> tuple[AdminItemBatchAdjustmentResult | None, tuple[str, ...]]:
        payload = self._payload(request)
        with DatabaseUnitOfWork(self.database, immediate=True) as uow:
            if not self._schema_ready(uow, action):
                return AdminItemBatchAdjustmentResult("not_ready", action), ()

            previous = uow.query_one(
                "SELECT payload,status,total FROM admin_item_batch_operations "
                "WHERE operation_id=?",
                (operation_id,),
            )
            legacy = None
            if previous is None and action == "grant":
                legacy = uow.query_one(
                    "SELECT substr(payload,1,?) AS payload_prefix,length(payload) AS payload_length,"
                    "status,total,completed,added FROM admin_item_batch_grant_operations "
                    "WHERE operation_id=?",
                    (self.payload_prefix_chars, operation_id),
                )
                if legacy is not None:
                    if not self._matches_request(str(legacy["payload_prefix"]), request):
                        return self._legacy_result(uow, operation_id, "operation_conflict"), ()
                    if str(legacy["status"]) == "completed":
                        return self._legacy_result(uow, operation_id, "duplicate"), ()

            if previous is not None and not self._matches_request(str(previous["payload"]), request):
                return self._result(uow, operation_id, "operation_conflict", action), ()
            if previous is not None and str(previous["status"]) == "completed":
                return self._result(uow, operation_id, "duplicate", action), ()

            if previous is None and legacy is not None:
                if int(legacy["payload_length"]) > self.max_legacy_payload_chars:
                    return self._legacy_result(uow, operation_id, "legacy_payload_too_large"), ()
                total = int(legacy["total"])
                if not self._has_space(total):
                    return self._legacy_result(uow, operation_id, "insufficient_space"), ()
                full = uow.query_one(
                    "SELECT payload FROM admin_item_batch_grant_operations WHERE operation_id=?",
                    (operation_id,),
                )
                uow.execute("SAVEPOINT admin_item_batch_legacy_import")
                uow.execute(
                    "INSERT INTO admin_item_batch_operations(operation_id,action,payload,total,status) "
                    "VALUES(?,'grant',?,?, 'running')",
                    (operation_id, payload, total),
                )
                self._insert_targets(
                    uow,
                    operation_id,
                    self._iter_legacy_users(str(full["payload"])),
                    self.target_insert_chunk_size,
                )
                self._import_legacy_progress(uow, operation_id)
                imported = uow.query_one(
                    "SELECT COUNT(*) AS total FROM admin_item_batch_targets "
                    "WHERE operation_id=?",
                    (operation_id,),
                )
                if int(imported["total"]) != total:
                    uow.execute("ROLLBACK TO admin_item_batch_legacy_import")
                    uow.execute("RELEASE admin_item_batch_legacy_import")
                    return AdminItemBatchAdjustmentResult("progress_corrupt", "grant", total), ()
                uow.execute("RELEASE admin_item_batch_legacy_import")
                previous = {"payload": payload, "status": "running", "total": total}

            if previous is None:
                running = uow.query_one(
                    "SELECT operation_id FROM admin_item_batch_operations "
                    "WHERE action=? AND status='running' AND payload=? "
                    "ORDER BY created_at DESC,rowid DESC LIMIT 1",
                    (action, payload),
                )
                if running is not None:
                    return self._result(
                        uow, str(running["operation_id"]), "in_progress", action
                    ), ()
                if action == "grant":
                    legacy_running = uow.query_one(
                        "SELECT operation_id FROM admin_item_batch_grant_operations "
                        "WHERE status='running' AND substr(payload,1,?)=? "
                        "ORDER BY created_at DESC,rowid DESC LIMIT 1",
                        (
                            len(self._legacy_payload_prefix(request)),
                            self._legacy_payload_prefix(request),
                        ),
                    )
                    if legacy_running is not None:
                        return self._legacy_result(
                            uow, str(legacy_running["operation_id"]), "in_progress"
                        ), ()
                roster = uow.query_one(
                    "SELECT COUNT(*) AS total,"
                    "COUNT(DISTINCT TRIM(CAST(user_id AS TEXT))) AS distinct_users,"
                    "SUM(CASE WHEN user_id IS NULL OR TRIM(CAST(user_id AS TEXT))='' "
                    "THEN 1 ELSE 0 END) AS invalid_users FROM user_xiuxian"
                )
                total = int(roster["distinct_users"] or 0)
                if int(roster["total"]) != total or int(roster["invalid_users"] or 0):
                    return AdminItemBatchAdjustmentResult("invalid_schema", action, total=total), ()
                if total <= 0:
                    return AdminItemBatchAdjustmentResult("no_targets", action), ()
                if not self._has_space(total):
                    return AdminItemBatchAdjustmentResult("insufficient_space", action, total), ()
                uow.execute(
                    "INSERT INTO admin_item_batch_operations(operation_id,action,payload,total,status) "
                    "VALUES(?,?,?,?, 'running')",
                    (operation_id, action, payload, total),
                )
                uow.execute(
                    "INSERT INTO admin_item_batch_targets"
                    "(operation_id,user_id,status,added_quantity,removed_quantity,result_json) "
                    "SELECT ?,TRIM(CAST(user_id AS TEXT)),'pending',0,0,'{}' FROM user_xiuxian "
                    "WHERE user_id IS NOT NULL AND TRIM(CAST(user_id AS TEXT))!='' "
                    "GROUP BY TRIM(CAST(user_id AS TEXT))",
                    (operation_id,),
                )
                inserted = uow.query_one(
                    "SELECT COUNT(*) AS total FROM admin_item_batch_targets WHERE operation_id=?",
                    (operation_id,),
                )
                if int(inserted["total"]) != total:
                    raise RuntimeError("admin item batch roster changed during snapshot")
                previous = {"payload": payload, "status": "running", "total": total}

            pending = uow.query_all(
                "SELECT user_id FROM admin_item_batch_targets "
                "WHERE operation_id=? AND status='pending' ORDER BY user_id LIMIT ?",
                (operation_id, chunk_size),
            )
            if not pending:
                completed = uow.query_one(
                    "SELECT COUNT(*) AS completed FROM admin_item_batch_targets "
                    "WHERE operation_id=? AND status!='pending'",
                    (operation_id,),
                )
                if int(completed["completed"]) < int(previous["total"]):
                    return self._result(uow, operation_id, "progress_corrupt", action), ()
                uow.execute(
                    "UPDATE admin_item_batch_operations SET status='completed',"
                    "updated_at=CURRENT_TIMESTAMP WHERE operation_id=?",
                    (operation_id,),
                )
            return None, tuple(str(row["user_id"]) for row in pending)

    def _grant_snapshot(self, user_id: str, item_id: int) -> tuple[str, int]:
        with DatabaseUnitOfWork(self.database, read_only=True) as uow:
            if uow.query_one("SELECT 1 AS present FROM user_xiuxian WHERE user_id=?", (user_id,)) is None:
                return "user_missing", 0
            row = uow.query_one(
                "SELECT COALESCE(goods_num,0) AS quantity FROM back WHERE user_id=? AND goods_id=?",
                (user_id, item_id),
            )
            return "ok", int(row["quantity"]) if row is not None else 0

    def _destroy_snapshot(self, user_id: str, item_id: int) -> tuple[str, int]:
        with DatabaseUnitOfWork(self.database, read_only=True) as uow:
            if uow.query_one("SELECT 1 AS present FROM user_xiuxian WHERE user_id=?", (user_id,)) is None:
                return "user_missing", 0
            row = uow.query_one(
                "SELECT COALESCE(goods_num,0) AS quantity FROM back WHERE user_id=? AND goods_id=?",
                (user_id, item_id),
            )
            return "ok", int(row["quantity"]) if row is not None else 0

    def _record(
        self,
        operation_id: str,
        action: str,
        user_id: str,
        status: str,
        added: int = 0,
        removed: int = 0,
    ) -> None:
        result_json = json.dumps(
            {"status": status, "added": int(added), "removed": int(removed)},
            ensure_ascii=True,
            sort_keys=True,
            separators=(",", ":"),
        )
        with DatabaseUnitOfWork(self.database, immediate=True) as uow:
            uow.execute(
                "UPDATE admin_item_batch_targets SET status=?,added_quantity=?,removed_quantity=?,"
                "result_json=? WHERE operation_id=? AND user_id=? AND status='pending'",
                (status, int(added), int(removed), result_json, operation_id, user_id),
            )
            operation = uow.query_one(
                "SELECT action,total FROM admin_item_batch_operations WHERE operation_id=?",
                (operation_id,),
            )
            counts = uow.query_one(
                "SELECT COUNT(*) AS completed,COALESCE(SUM(added_quantity),0) AS added,"
                "COALESCE(SUM(removed_quantity),0) AS removed FROM admin_item_batch_targets "
                "WHERE operation_id=? AND status!='pending'",
                (operation_id,),
            )
            legacy = None
            if operation is not None and str(operation["action"]) == "grant":
                legacy = uow.query_one(
                    "SELECT 1 AS present FROM admin_item_batch_grant_operations "
                    "WHERE operation_id=?",
                    (operation_id,),
                )
            if legacy is not None:
                uow.execute(
                    "INSERT OR IGNORE INTO admin_item_batch_grant_progress"
                    "(operation_id,user_id,added) VALUES(?,?,?)",
                    (operation_id, user_id, int(added)),
                )
                old_status = "completed" if int(counts["completed"]) >= int(operation["total"]) else "running"
                uow.execute(
                    "UPDATE admin_item_batch_grant_operations SET completed=?,added=?,status=?,"
                    "updated_at=CURRENT_TIMESTAMP WHERE operation_id=?",
                    (int(counts["completed"]), int(counts["added"]), old_status, operation_id),
                )
            if operation is not None:
                new_status = "completed" if int(counts["completed"]) >= int(operation["total"]) else "running"
                uow.execute(
                    "UPDATE admin_item_batch_operations SET status=?,updated_at=CURRENT_TIMESTAMP "
                    "WHERE operation_id=?",
                    (new_status, operation_id),
                )

    def adjust(
        self,
        action: str,
        operation_id: str,
        operator_id: str,
        item_id: int,
        item_name: str,
        item_type: str,
        quantity: int,
        max_goods_num: int = 0,
        *,
        chunk_size: int = 100,
    ) -> AdminItemBatchAdjustmentResult:
        action = str(action).strip()
        operation_id = str(operation_id).strip()
        operator_id = str(operator_id).strip()
        item_id, quantity, max_goods_num = int(item_id), int(quantity), int(max_goods_num)
        chunk_size = min(self.max_chunk_size, max(1, int(chunk_size)))
        if (
            action not in {"grant", "destroy"}
            or not operation_id
            or not operator_id
            or item_id <= 0
            or quantity <= 0
            or (action == "grant" and max_goods_num <= 0)
        ):
            raise ValueError("invalid admin item batch arguments")
        if not self.database.is_file():
            return AdminItemBatchAdjustmentResult("not_ready", action)

        request = self._request(
            action, operator_id, item_id, item_name, item_type, quantity, max_goods_num
        )
        previous, pending = self._begin(operation_id, action, request, chunk_size)
        if previous is not None:
            return previous

        for user_id in pending:
            if action == "grant":
                result: AdminItemResult | None = None
                for _ in range(3):
                    snapshot_status, expected = self._grant_snapshot(user_id, item_id)
                    if snapshot_status == "user_missing":
                        result = AdminItemResult("user_missing", user_id, item_id)
                        break
                    result = self.item_repository.grant(
                        f"admin-item-batch:{operation_id}:grant:{user_id}",
                        operator_id,
                        user_id,
                        item_id,
                        item_name,
                        item_type,
                        quantity,
                        expected,
                        max_goods_num,
                        require_user=True,
                        audit_action="admin_item_add_all",
                        audit_trace_id=operation_id,
                        target_name="all",
                    )
                    if result.status == "schema_missing":
                        return AdminItemBatchAdjustmentResult("not_ready", action)
                    if result.status != "state_changed":
                        break
                if result is None:
                    raise RuntimeError("admin item grant returned no result")
                if result.status == "state_changed":
                    status = "state_changed"
                elif result.status == "operation_conflict":
                    raise RuntimeError(f"admin item batch child operation conflict: {user_id}")
                else:
                    status = result.status
                self._record(
                    operation_id,
                    action,
                    user_id,
                    status,
                    int(result.granted_quantity or 0),
                )
            else:
                result = None
                for _ in range(3):
                    snapshot_status, expected = self._destroy_snapshot(user_id, item_id)
                    if snapshot_status == "user_missing":
                        result = AdminItemDestroyResult("user_missing")
                        break
                    if expected <= 0:
                        result = AdminItemDestroyResult("item_missing")
                        break
                    result = self.destroy_repository.destroy(
                        f"admin-item-batch:{operation_id}:destroy:{user_id}",
                        operator_id,
                        user_id,
                        item_id,
                        item_name,
                        item_type,
                        quantity,
                        expected,
                        target_name="all",
                        audit_action="admin_item_cost_all",
                        audit_trace_id=operation_id,
                    )
                    if result.status == "schema_missing":
                        return AdminItemBatchAdjustmentResult("not_ready", action)
                    if result.status != "state_changed":
                        break
                if result is None:
                    raise RuntimeError("admin item destroy returned no result")
                if result.status == "state_changed":
                    status = "state_changed"
                elif result.status == "operation_conflict":
                    raise RuntimeError(f"admin item batch child operation conflict: {user_id}")
                else:
                    status = result.status
                self._record(
                    operation_id,
                    action,
                    user_id,
                    status,
                    removed=int(result.removed_quantity or 0),
                )

        with DatabaseUnitOfWork(self.database, read_only=True) as uow:
            return self._result(uow, operation_id, "applied", action)


__all__ = ["AdminItemBatchAdjustmentResult", "AdminItemBatchSqlRepository"]
