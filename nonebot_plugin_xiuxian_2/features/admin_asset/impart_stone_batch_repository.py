from __future__ import annotations

import json
import shutil
import sqlite3
from collections.abc import Iterable
from dataclasses import dataclass
from pathlib import Path
from typing import Any

from ...infrastructure.database import DatabaseUnitOfWork
from .impart_stone_repository import (
    AdminImpartStoneResult,
    AdminImpartStoneSqlRepository,
)


@dataclass(frozen=True)
class AdminImpartStoneBatchAdjustmentResult:
    status: str
    total: int = 0
    completed: int = 0
    applied_delta: int = 0
    affected_users: int = 0
    skipped_users: int = 0

    @property
    def succeeded(self) -> bool:
        return self.status in {"applied", "duplicate"}


class AdminImpartStoneBatchSqlRepository:
    """Freeze the roster in game DB and advance cross-database receipts in bounded chunks."""

    reserve_bytes = 8 * 1024 * 1024
    bytes_per_target = 2048
    target_insert_chunk_size = 500
    max_chunk_size = 100
    payload_prefix_chars = 4096
    max_legacy_payload_chars = 64 * 1024 * 1024

    def __init__(
        self,
        game_database: str | Path,
        impart_database: str | Path,
        stone_repository: AdminImpartStoneSqlRepository | None = None,
    ) -> None:
        self.game_database = Path(game_database)
        self.impart_database = Path(impart_database)
        self.stone_repository = stone_repository or AdminImpartStoneSqlRepository(
            game_database, impart_database
        )

    @staticmethod
    def _request(operator_id: str, requested_delta: int) -> dict[str, Any]:
        return {
            "operator_id": str(operator_id).strip(),
            "requested_delta": int(requested_delta),
        }

    @staticmethod
    def _payload(request: dict[str, Any]) -> str:
        return json.dumps(
            {"request": request},
            ensure_ascii=True,
            sort_keys=True,
            separators=(",", ":"),
        )

    @staticmethod
    def _payload_request(payload_prefix: str) -> tuple[dict[str, Any] | None, bool]:
        marker = '{"request":'
        if not payload_prefix.startswith(marker):
            return None, False
        try:
            request, end = json.JSONDecoder().raw_decode(payload_prefix, len(marker))
        except (TypeError, ValueError, json.JSONDecodeError):
            return None, False
        if not isinstance(request, dict):
            return None, False
        return request, payload_prefix[end:].startswith(',"users":[')

    @classmethod
    def _matches_request(cls, payload_prefix: str, request: dict[str, Any]) -> bool:
        payload_request, _ = cls._payload_request(payload_prefix)
        return payload_request == request

    @staticmethod
    def _schema_ready(uow: DatabaseUnitOfWork) -> bool:
        required = {
            "user_xiuxian": {"user_id"},
            "admin_impart_stone_operations": {
                "operation_id", "payload", "previous_stone", "final_stone", "applied_delta",
            },
            "economy_log": {
                "user_id", "source", "action", "item_delta", "detail", "trace_id", "created_at",
            },
            "admin_impart_stone_batch_operations": {
                "operation_id", "payload", "total", "status", "created_at", "updated_at",
            },
            "admin_impart_stone_batch_progress": {
                "operation_id", "user_id", "status", "applied_delta", "result_json",
            },
            "admin_impart_stone_batch_targets": {
                "operation_id", "user_id", "status", "applied_delta", "result_json",
            },
        }
        for table, required_columns in required.items():
            columns = {
                str(row["name"]).casefold()
                for row in uow.query_all(f'PRAGMA table_info("{table}")')
            }
            if not required_columns.issubset(columns):
                return False
        return True

    def _impart_schema_ready(self) -> bool:
        if not self.impart_database.is_file():
            return False
        try:
            with DatabaseUnitOfWork(self.impart_database, read_only=True) as uow:
                columns = {
                    str(row["name"]).casefold()
                    for row in uow.query_all('PRAGMA table_info("xiuxian_impart")')
                }
                return {"user_id", "stone_num"}.issubset(columns)
        except (OSError, ValueError, sqlite3.Error):
            return False

    def _has_space(self, required: int) -> bool:
        try:
            game_free = shutil.disk_usage(self.game_database.parent).free
            impart_free = shutil.disk_usage(self.impart_database.parent).free
        except OSError:
            return False
        return min(game_free, impart_free) >= required

    def _required_bytes(self, target_count: int) -> int:
        return self.reserve_bytes + max(0, target_count) * self.bytes_per_target

    @classmethod
    def _result(
        cls,
        uow: DatabaseUnitOfWork,
        operation_id: str,
        status: str,
    ) -> AdminImpartStoneBatchAdjustmentResult:
        operation = uow.query_one(
            "SELECT total FROM admin_impart_stone_batch_operations WHERE operation_id=?",
            (operation_id,),
        )
        if operation is None:
            return AdminImpartStoneBatchAdjustmentResult(status)
        target_count = uow.query_one(
            "SELECT COUNT(*) AS total FROM admin_impart_stone_batch_targets "
            "WHERE operation_id=?",
            (operation_id,),
        )
        if int(target_count["total"]) > 0:
            counts = uow.query_one(
                "SELECT SUM(CASE WHEN status!='pending' THEN 1 ELSE 0 END) AS completed,"
                "COALESCE(SUM(applied_delta),0) AS applied_delta,"
                "SUM(CASE WHEN status!='pending' AND applied_delta!=0 THEN 1 ELSE 0 END) "
                "AS affected_users FROM admin_impart_stone_batch_targets "
                "WHERE operation_id=?",
                (operation_id,),
            )
        else:
            counts = uow.query_one(
                "SELECT COUNT(*) AS completed,COALESCE(SUM(applied_delta),0) AS applied_delta,"
                "SUM(CASE WHEN applied_delta!=0 THEN 1 ELSE 0 END) AS affected_users "
                "FROM admin_impart_stone_batch_progress WHERE operation_id=?",
                (operation_id,),
            )
        completed = int(counts["completed"] or 0)
        affected_users = int(counts["affected_users"] or 0)
        return AdminImpartStoneBatchAdjustmentResult(
            status,
            int(operation["total"]),
            completed,
            int(counts["applied_delta"] or 0),
            affected_users,
            completed - affected_users,
        )

    def find_running(self, operator_id: str, requested_delta: int) -> str | None:
        if not self.game_database.is_file():
            return None
        request = self._request(operator_id, requested_delta)
        try:
            with DatabaseUnitOfWork(self.game_database, read_only=True) as uow:
                exists = uow.query_one(
                    "SELECT 1 AS present FROM sqlite_master WHERE type='table' "
                    "AND name='admin_impart_stone_batch_operations'"
                )
                if exists is None:
                    return None
                rows = uow.query_all(
                    "SELECT operation_id,substr(payload,1,?) AS payload_prefix "
                    "FROM admin_impart_stone_batch_operations "
                    "WHERE status='running' ORDER BY created_at DESC,rowid DESC",
                    (self.payload_prefix_chars,),
                )
                for row in rows:
                    if self._matches_request(str(row["payload_prefix"]), request):
                        return str(row["operation_id"])
        except (OSError, ValueError, sqlite3.Error):
            return None
        return None

    @classmethod
    def _insert_legacy_targets(
        cls,
        uow: DatabaseUnitOfWork,
        operation_id: str,
        user_ids: Iterable[Any],
    ) -> None:
        batch: list[tuple[str, str]] = []
        for raw_user_id in user_ids:
            user_id = str(raw_user_id).strip()
            if not user_id:
                continue
            batch.append((operation_id, user_id))
            if len(batch) >= cls.target_insert_chunk_size:
                uow.executemany(
                    "INSERT OR IGNORE INTO admin_impart_stone_batch_targets"
                    "(operation_id,user_id,status,applied_delta,result_json) "
                    "VALUES(?,?,'pending',0,'{}')",
                    batch,
                )
                batch.clear()
        if batch:
            uow.executemany(
                "INSERT OR IGNORE INTO admin_impart_stone_batch_targets"
                "(operation_id,user_id,status,applied_delta,result_json) "
                "VALUES(?,?,'pending',0,'{}')",
                batch,
            )
        uow.execute(
            "UPDATE admin_impart_stone_batch_targets AS t SET "
            "status=(SELECT p.status FROM admin_impart_stone_batch_progress p "
            "WHERE p.operation_id=t.operation_id AND p.user_id=t.user_id),"
            "applied_delta=(SELECT p.applied_delta FROM admin_impart_stone_batch_progress p "
            "WHERE p.operation_id=t.operation_id AND p.user_id=t.user_id),"
            "result_json=(SELECT p.result_json FROM admin_impart_stone_batch_progress p "
            "WHERE p.operation_id=t.operation_id AND p.user_id=t.user_id) "
            "WHERE t.operation_id=? AND EXISTS (SELECT 1 FROM admin_impart_stone_batch_progress p "
            "WHERE p.operation_id=t.operation_id AND p.user_id=t.user_id)",
            (operation_id,),
        )

    @staticmethod
    def _iter_legacy_users(payload: str) -> Iterable[Any]:
        request_marker = '{"request":'
        decoder = json.JSONDecoder()
        try:
            _, index = decoder.raw_decode(payload, len(request_marker))
        except (TypeError, ValueError, json.JSONDecodeError) as exc:
            raise ValueError("invalid legacy impart stone batch payload") from exc
        users_marker = ',"users":['
        if not payload.startswith(users_marker, index):
            raise ValueError("legacy impart stone batch has no frozen users")
        index += len(users_marker)
        while True:
            while index < len(payload) and payload[index].isspace():
                index += 1
            if index >= len(payload):
                raise ValueError("truncated legacy impart stone batch users")
            if payload[index] == "]":
                return
            try:
                user_id, index = decoder.raw_decode(payload, index)
            except (TypeError, ValueError, json.JSONDecodeError) as exc:
                raise ValueError("invalid legacy impart stone batch user") from exc
            yield user_id
            while index < len(payload) and payload[index].isspace():
                index += 1
            if index >= len(payload):
                raise ValueError("truncated legacy impart stone batch users")
            if payload[index] == ",":
                index += 1
                continue
            if payload[index] == "]":
                return
            raise ValueError("invalid legacy impart stone batch user separator")

    def _begin(
        self,
        operation_id: str,
        request: dict[str, Any],
        chunk_size: int,
    ) -> tuple[AdminImpartStoneBatchAdjustmentResult | None, tuple[str, ...]]:
        payload = self._payload(request)
        with DatabaseUnitOfWork(self.game_database, immediate=True) as uow:
            if not self._schema_ready(uow):
                return AdminImpartStoneBatchAdjustmentResult("not_ready"), ()

            previous = uow.query_one(
                "SELECT substr(payload,1,?) AS payload_prefix,length(payload) AS payload_length,"
                "status,total FROM admin_impart_stone_batch_operations "
                "WHERE operation_id=?",
                (self.payload_prefix_chars, operation_id),
            )
            if previous is not None and not self._matches_request(
                str(previous["payload_prefix"]), request
            ):
                return self._result(uow, operation_id, "operation_conflict"), ()
            if previous is not None and str(previous["status"]) == "completed":
                return self._result(uow, operation_id, "duplicate"), ()
            if not self._impart_schema_ready():
                return AdminImpartStoneBatchAdjustmentResult("not_ready"), ()

            if previous is None:
                running = uow.query_all(
                    "SELECT operation_id,substr(payload,1,?) AS payload_prefix "
                    "FROM admin_impart_stone_batch_operations "
                    "WHERE status='running' ORDER BY created_at DESC,rowid DESC",
                    (self.payload_prefix_chars,),
                )
                for row in running:
                    if self._matches_request(str(row["payload_prefix"]), request):
                        return self._result(uow, str(row["operation_id"]), "in_progress"), ()

                roster = uow.query_one(
                    "SELECT COUNT(*) AS total,"
                    "COUNT(DISTINCT TRIM(CAST(user_id AS TEXT))) AS distinct_users,"
                    "SUM(CASE WHEN user_id IS NULL OR TRIM(CAST(user_id AS TEXT))='' "
                    "THEN 1 ELSE 0 END) AS invalid_users FROM user_xiuxian"
                )
                total = int(roster["distinct_users"] or 0)
                if int(roster["total"]) != total or int(roster["invalid_users"] or 0):
                    return AdminImpartStoneBatchAdjustmentResult("invalid_schema", total=total), ()
                if total <= 0:
                    return AdminImpartStoneBatchAdjustmentResult("no_targets"), ()
                if not self._has_space(self._required_bytes(total)):
                    return AdminImpartStoneBatchAdjustmentResult(
                        "insufficient_space", total=total
                    ), ()

                uow.execute(
                    "INSERT INTO admin_impart_stone_batch_operations"
                    "(operation_id,payload,total,status) VALUES(?,?,?,'running')",
                    (operation_id, payload, total),
                )
                uow.execute(
                    "INSERT INTO admin_impart_stone_batch_targets"
                    "(operation_id,user_id,status,applied_delta,result_json) "
                    "SELECT ?,TRIM(CAST(user_id AS TEXT)),'pending',0,'{}' "
                    "FROM user_xiuxian WHERE user_id IS NOT NULL "
                    "AND TRIM(CAST(user_id AS TEXT))!='' "
                    "GROUP BY TRIM(CAST(user_id AS TEXT))",
                    (operation_id,),
                )
                inserted = uow.query_one(
                    "SELECT COUNT(*) AS total FROM admin_impart_stone_batch_targets "
                    "WHERE operation_id=?",
                    (operation_id,),
                )
                if int(inserted["total"]) != total:
                    raise RuntimeError("impart stone batch target snapshot changed")
                previous = {
                    "payload_prefix": payload,
                    "payload_length": len(payload),
                    "status": "running",
                    "total": total,
                }
            _, has_legacy_users = self._payload_request(str(previous["payload_prefix"]))
            if has_legacy_users:
                target_count = uow.query_one(
                    "SELECT COUNT(*) AS total FROM admin_impart_stone_batch_targets "
                    "WHERE operation_id=?",
                    (operation_id,),
                )
                if int(target_count["total"]) == 0 and int(previous["total"]) > 0:
                    if int(previous["payload_length"]) > self.max_legacy_payload_chars:
                        return self._result(uow, operation_id, "legacy_payload_too_large"), ()
                    if not self._has_space(self._required_bytes(int(previous["total"]))):
                        return self._result(uow, operation_id, "insufficient_space"), ()
                    full_payload = uow.query_one(
                        "SELECT payload FROM admin_impart_stone_batch_operations "
                        "WHERE operation_id=?",
                        (operation_id,),
                    )
                    self._insert_legacy_targets(
                        uow, operation_id,
                        self._iter_legacy_users(str(full_payload["payload"])),
                    )

            pending = uow.query_all(
                "SELECT user_id FROM admin_impart_stone_batch_targets "
                "WHERE operation_id=? AND status='pending' ORDER BY user_id LIMIT ?",
                (operation_id, chunk_size),
            )
            if not pending:
                progress = uow.query_one(
                    "SELECT COUNT(*) AS completed FROM admin_impart_stone_batch_targets "
                    "WHERE operation_id=? AND status!='pending'",
                    (operation_id,),
                )
                if int(progress["completed"]) < int(previous["total"]):
                    return self._result(uow, operation_id, "progress_corrupt"), ()
            return None, tuple(str(row["user_id"]) for row in pending)

    def _record(self, operation_id: str, user_id: str, result: AdminImpartStoneResult) -> None:
        result_json = json.dumps(
            {
                "status": result.status,
                "previous_stone": result.previous_stone,
                "final_stone": result.final_stone,
                "applied_delta": result.applied_delta,
            },
            ensure_ascii=True,
            sort_keys=True,
            separators=(",", ":"),
        )
        with DatabaseUnitOfWork(self.game_database, immediate=True) as uow:
            uow.execute(
                "UPDATE admin_impart_stone_batch_targets SET status=?,applied_delta=?,"
                "result_json=? WHERE operation_id=? AND user_id=? AND status='pending'",
                (result.status, result.applied_delta, result_json, operation_id, user_id),
            )
            uow.execute(
                "INSERT INTO admin_impart_stone_batch_progress"
                "(operation_id,user_id,status,applied_delta,result_json) VALUES(?,?,?,?,?) "
                "ON CONFLICT(operation_id,user_id) DO UPDATE SET status=excluded.status,"
                "applied_delta=excluded.applied_delta,result_json=excluded.result_json "
                "WHERE ABS(excluded.applied_delta)>"
                "ABS(admin_impart_stone_batch_progress.applied_delta)",
                (operation_id, user_id, result.status, result.applied_delta, result_json),
            )
            counts = uow.query_one(
                "SELECT total,(SELECT COUNT(*) FROM admin_impart_stone_batch_targets t "
                "WHERE t.operation_id=o.operation_id AND t.status!='pending') AS completed "
                "FROM admin_impart_stone_batch_operations o WHERE operation_id=?",
                (operation_id,),
            )
            if counts is not None:
                status = "completed" if int(counts["completed"]) >= int(counts["total"]) else "running"
                uow.execute(
                    "UPDATE admin_impart_stone_batch_operations SET status=?,"
                    "updated_at=CURRENT_TIMESTAMP WHERE operation_id=?",
                    (status, operation_id),
                )

    def adjust(
        self,
        operation_id: str,
        operator_id: str,
        requested_delta: int,
        *,
        chunk_size: int = 100,
    ) -> AdminImpartStoneBatchAdjustmentResult:
        operation_id = str(operation_id).strip()
        request = self._request(operator_id, requested_delta)
        chunk_size = min(self.max_chunk_size, max(1, int(chunk_size)))
        if not operation_id or not request["operator_id"] or not request["requested_delta"]:
            raise ValueError("invalid impart stone batch arguments")
        if not self.game_database.is_file() or not self.impart_database.is_file():
            return AdminImpartStoneBatchAdjustmentResult("not_ready")

        previous, pending = self._begin(operation_id, request, chunk_size)
        if previous is not None:
            return previous

        for user_id in pending:
            result = None
            for _ in range(3):
                snapshot = self.stone_repository.snapshot(user_id)
                if snapshot.status == "schema_missing":
                    return AdminImpartStoneBatchAdjustmentResult("not_ready")
                if snapshot.status != "ok":
                    result = AdminImpartStoneResult(snapshot.status)
                    break
                result = self.stone_repository.adjust(
                    f"admin-impart-stone-batch:{operation_id}:{user_id}",
                    request["operator_id"],
                    user_id,
                    snapshot.stone,
                    request["requested_delta"],
                    target_name="all",
                )
                if result.status == "schema_missing":
                    return AdminImpartStoneBatchAdjustmentResult("not_ready")
                if result.status != "state_changed":
                    break
            if result is None:
                raise RuntimeError("impart stone adjustment returned no result")
            if result.status == "operation_conflict":
                raise RuntimeError(f"impart stone batch child operation conflict: {user_id}")
            self._record(operation_id, user_id, result)

        with DatabaseUnitOfWork(self.game_database, read_only=True) as uow:
            return self._result(uow, operation_id, "applied")


__all__ = [
    "AdminImpartStoneBatchAdjustmentResult",
    "AdminImpartStoneBatchSqlRepository",
]
