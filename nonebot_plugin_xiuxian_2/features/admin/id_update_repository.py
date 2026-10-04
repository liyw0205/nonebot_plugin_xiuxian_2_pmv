from __future__ import annotations

import json
from datetime import datetime, timezone
import os
from pathlib import Path
from typing import Any, Callable, Iterator, Mapping

from ...core.errors import OperationConflictError
from ...core.result import OperationOutcome
from ...infrastructure.database import DatabaseUnitOfWork, OperationLedger, OutboxStore
from ...infrastructure.database.backup_capacity import preflight_capacity
from ...infrastructure.database.ledger import request_hash
from .id_mutation_lock import exclusive_id_mutation_lock
from .id_swap_repository import DATABASE_ORDER, TARGET_COLUMNS, _quote_ident


ACTION = "admin.id-update"
MAX_ID_BYTES = 255


class _SchemaMissing(RuntimeError):
    pass


def _now() -> str:
    return datetime.now(timezone.utc).isoformat()


def _entry_exists(path: Path) -> bool:
    return path.exists() or path.is_symlink()


class AdminIdUpdateSqlRepository:
    """Recoverable single-direction ID migration across legacy SQLite files."""

    def __init__(
        self,
        databases: Mapping[str, str | Path],
        players_dir: str | Path,
        *,
        invalidate_user_id_cache: Callable[[], Any] | None = None,
        invalidate_player_data_cache: Callable[[str, tuple[str, ...]], Any] | None = None,
    ) -> None:
        self.databases = {key: Path(databases[key]) for key in DATABASE_ORDER}
        self.players_dir = Path(players_dir)
        self.invalidate_user_id_cache = invalidate_user_id_cache or (lambda: None)
        self.invalidate_player_data_cache = invalidate_player_data_cache or (lambda *_args: None)
        self.ledger = OperationLedger()
        self.outbox = OutboxStore()

    def update(
        self, operation_id: str, old_id: str, new_id: str
    ) -> OperationOutcome[dict[str, Any]]:
        operation_id, old_id, new_id = map(str.strip, (str(operation_id), str(old_id), str(new_id)))
        if not operation_id:
            return self._rejected(operation_id, "operation_id 不能为空", "invalid_operation")
        rejection = self._validate_ids(old_id, new_id)
        if rejection is not None:
            return self._rejected(operation_id, rejection, "invalid_id")
        if not all(self.databases[key].is_file() for key in DATABASE_ORDER):
            return OperationOutcome.failed(
                operation_id,
                ACTION,
                "ID更新迁移尚未就绪：缺少一个或多个数据库文件。",
                code="schema_missing",
                audit_category="admin",
            )

        with exclusive_id_mutation_lock(self.databases["game_db"]):
            try:
                plan, terminal = self._prepare(operation_id, old_id, new_id)
            except Exception as exc:
                plan = self._load_plan(operation_id)
                if plan is not None:
                    return self._mark_needs_reconcile(plan, exc)
                schema_missing = isinstance(exc, _SchemaMissing)
                return OperationOutcome.failed(
                    operation_id,
                    ACTION,
                    f"ID更新未执行：{str(exc)[:500]}",
                    code="schema_missing" if schema_missing else "schema_or_storage_error",
                    audit_category="admin",
                )
            if terminal is not None:
                return terminal
            if plan is None:
                return OperationOutcome.failed(
                    operation_id,
                    ACTION,
                    "ID更新计划未能建立。",
                    code="operation_plan_missing",
                    audit_category="admin",
                )
            try:
                self._preflight_write_capacity()
            except OSError as exc:
                return OperationOutcome.failed(
                    operation_id,
                    ACTION,
                    f"磁盘容量不足或无法确认，ID更新尚未开始或已等待恢复：{str(exc)[:500]}",
                    code=getattr(exc, "code", "storage_capacity_unavailable"),
                    data={"status": "started"},
                    audit_category="admin",
                )
            try:
                return self._resume(plan)
            except Exception as exc:
                return self._mark_needs_reconcile(plan, exc)

    @staticmethod
    def _validate_ids(old_id: str, new_id: str) -> str | None:
        if not old_id or not new_id:
            return "参数错误：ID1 和 ID2 不能为空"
        if old_id == new_id:
            return "ID1 与 ID2 相同，无需更新"
        for value in (old_id, new_id):
            try:
                encoded_size = len(value.encode("utf-8"))
            except UnicodeEncodeError:
                return "ID格式无效。"
            if (
                value in {".", ".."}
                or "/" in value
                or "\\" in value
                or ":" in value
                or any(ord(char) < 32 or ord(char) == 127 for char in value)
                or encoded_size > MAX_ID_BYTES
            ):
                return "ID格式不支持：ID必须是单个目录名且不能超过255个UTF-8字节。"
        return None

    @staticmethod
    def _rejected(operation_id: str, message: str, code: str) -> OperationOutcome[dict[str, Any]]:
        return OperationOutcome.rejected(
            operation_id,
            ACTION,
            message,
            code=code,
            data={"status": "rejected"},
            audit_category="admin",
        )

    def _schema_ready(self, uow: DatabaseUnitOfWork, database_key: str) -> bool:
        required = {"admin_id_update_step_receipts"}
        if database_key == "game_db":
            required.update(
                {
                    "admin_id_update_operations",
                    "admin_id_swap_operations",
                    "operation_ledger",
                    "operation_audit",
                    "domain_outbox",
                }
            )
        rows = uow.query_all(
            "SELECT name FROM sqlite_master WHERE type='table' AND name IN ("
            + ",".join("?" for _ in required)
            + ")",
            tuple(sorted(required)),
        )
        return {str(row["name"]) for row in rows} == required

    @staticmethod
    def _id_table_targets(uow: DatabaseUnitOfWork, database_key: str) -> Iterator[tuple[str, str]]:
        tables = uow.query_all(
            "SELECT name FROM sqlite_master WHERE type='table' "
            "AND name NOT LIKE 'sqlite_%' ORDER BY name"
        )
        for row in tables:
            table = str(row["name"])
            columns = {
                str(column["name"])
                for column in uow.query_all(f"PRAGMA table_info({_quote_ident(table)})")
            }
            for column in sorted(columns.intersection(TARGET_COLUMNS[database_key])):
                yield table, column

    def _target_snapshot(
        self, database_key: str, old_id: str, new_id: str
    ) -> tuple[list[tuple[str, str]], bool, bool]:
        with DatabaseUnitOfWork(self.databases[database_key], read_only=True) as uow:
            if not self._schema_ready(uow, database_key):
                raise _SchemaMissing(f"{database_key} ID更新迁移尚未就绪")
            targets = list(self._id_table_targets(uow, database_key))
            found_old = False
            found_new = False
            for table, column in targets:
                sql = f"SELECT 1 AS present FROM {_quote_ident(table)} WHERE CAST({_quote_ident(column)} AS TEXT)=? LIMIT 1"
                if not found_old and uow.query_one(sql, (old_id,)) is not None:
                    found_old = True
                if not found_new and uow.query_one(sql, (new_id,)) is not None:
                    found_new = True
                if found_old and found_new:
                    break
            return targets, found_old, found_new

    def _prepare(
        self, operation_id: str, old_id: str, new_id: str
    ) -> tuple[dict[str, Any] | None, OperationOutcome[dict[str, Any]] | None]:
        payload = {"old_id": old_id, "new_id": new_id}
        payload_digest = request_hash(payload)
        snapshots: dict[str, list[tuple[str, str]]] = {}
        found_old = found_new = False
        for database_key in DATABASE_ORDER:
            targets, has_old, has_new = self._target_snapshot(database_key, old_id, new_id)
            snapshots[database_key] = targets
            found_old = found_old or has_old
            found_new = found_new or has_new

        old_path, new_path = self.players_dir / old_id, self.players_dir / new_id
        directory_old_present = _entry_exists(old_path)
        if _entry_exists(new_path):
            found_new = True

        with DatabaseUnitOfWork(self.databases["game_db"], immediate=True) as game_uow:
            if not self._schema_ready(game_uow, "game_db"):
                return None, OperationOutcome.failed(
                    operation_id,
                    ACTION,
                    "ID更新迁移尚未就绪：请先完成启动迁移。",
                    code="schema_missing",
                    audit_category="admin",
                )
            previous = self.ledger.get(game_uow, operation_id, ACTION)
            try:
                self.ledger.begin(game_uow, operation_id, ACTION, payload)
            except OperationConflictError:
                return None, self._rejected(
                    operation_id,
                    "同一操作号对应了不同的 ID 参数，已拒绝重放。",
                    "operation_payload_conflict",
                )
            if previous is not None and previous.status in {"applied", "rejected"}:
                outcome = previous.outcome()
                return None, outcome.replay() if outcome is not None else OperationOutcome.failed(
                    operation_id,
                    ACTION,
                    "ID更新操作回执缺少结果，需人工检查。",
                    code="operation_receipt_invalid",
                    audit_category="admin",
                )

            plan = game_uow.query_one(
                "SELECT * FROM admin_id_update_operations WHERE operation_id=?",
                (operation_id,),
            )
            if plan is not None:
                if str(plan["payload_hash"]) != payload_digest:
                    return None, self._rejected(
                        operation_id,
                        "同一操作号对应了不同的 ID 参数，已拒绝重放。",
                        "operation_payload_conflict",
                    )
                self.outbox.append(
                    game_uow,
                    event_id=f"{operation_id}:{ACTION}",
                    aggregate_type="admin_id_update",
                    aggregate_id=operation_id,
                    event_type=ACTION,
                    payload=payload,
                )
                return dict(plan), None

            self.outbox.append(
                game_uow,
                event_id=f"{operation_id}:{ACTION}",
                aggregate_type="admin_id_update",
                aggregate_id=operation_id,
                event_type=ACTION,
                payload=payload,
            )
            pending_swap = game_uow.query_one(
                "SELECT operation_id FROM admin_id_swap_operations "
                "WHERE status IN ('started','needs_reconcile') LIMIT 1"
            )
            pending_update = game_uow.query_one(
                "SELECT operation_id FROM admin_id_update_operations "
                "WHERE status IN ('started','needs_reconcile') LIMIT 1"
            )
            if pending_swap is not None:
                return None, self._finish_rejected(
                    game_uow,
                    operation_id,
                    f"另一笔 ID交换仍待恢复：{pending_swap['operation_id']}。恢复完成前不能开始更新。",
                    "reconcile_pending",
                )
            if pending_update is not None:
                return None, self._finish_rejected(
                    game_uow,
                    operation_id,
                    f"另一笔 ID更新仍待恢复：{pending_update['operation_id']}。恢复完成前不能开始更新。",
                    "reconcile_pending",
                )
            if not found_old:
                return None, self._finish_rejected(game_uow, operation_id, "旧 ID 不存在，未执行更新。", "user_id_missing")
            if found_new:
                return None, self._finish_rejected(
                    game_uow, operation_id, "新 ID 已存在或其玩家目录已占用，禁止覆盖。", "user_id_conflict"
                )
            target_json = json.dumps(
                {key: [list(target) for target in values] for key, values in snapshots.items()},
                ensure_ascii=False,
                separators=(",", ":"),
            )
            values = {
                "operation_id": operation_id,
                "payload_hash": payload_digest,
                "old_id": old_id,
                "new_id": new_id,
                "target_columns_json": target_json,
                "directory_old_present": int(directory_old_present),
                "directory_phase": "pending",
                "status": "started",
                "completed_databases": "[]",
                "last_error": "",
                "created_at": _now(),
                "updated_at": _now(),
            }
            game_uow.execute(
                "INSERT INTO admin_id_update_operations "
                "(operation_id,payload_hash,old_id,new_id,target_columns_json,directory_old_present,"
                "directory_phase,status,completed_databases,last_error,created_at,updated_at) "
                "VALUES (:operation_id,:payload_hash,:old_id,:new_id,:target_columns_json,:directory_old_present,"
                ":directory_phase,:status,:completed_databases,:last_error,:created_at,:updated_at)",
                values,
            )
            return values, None

    def _finish_rejected(
        self,
        uow: DatabaseUnitOfWork,
        operation_id: str,
        message: str,
        code: str,
    ) -> OperationOutcome[dict[str, Any]]:
        outcome = OperationOutcome.rejected(
            operation_id,
            ACTION,
            message,
            code=code,
            data={"status": "rejected"},
            audit_category="admin",
        )
        self.ledger.finish(uow, outcome)
        self.outbox.mark_sent(uow, f"{operation_id}:{ACTION}")
        return outcome

    def _preflight_write_capacity(self) -> None:
        requirements = {}
        for path in set(self.databases.values()):
            size = max(1, path.stat().st_size)
            wal_path = Path(f"{path}-wal")
            if wal_path.exists():
                size += wal_path.stat().st_size
            shm_path = Path(f"{path}-shm")
            if shm_path.exists():
                size += shm_path.stat().st_size
            requirements[path] = size * 3
        preflight_capacity(requirements, operation="admin ID update WAL workspace")

    def _load_plan(self, operation_id: str) -> dict[str, Any] | None:
        try:
            with DatabaseUnitOfWork(self.databases["game_db"], read_only=True) as uow:
                row = uow.query_one(
                    "SELECT * FROM admin_id_update_operations WHERE operation_id=?",
                    (operation_id,),
                )
            return dict(row) if row is not None else None
        except Exception:
            return None

    def _resume(self, plan: Mapping[str, Any]) -> OperationOutcome[dict[str, Any]]:
        operation_id = str(plan["operation_id"])
        old_id, new_id = str(plan["old_id"]), str(plan["new_id"])
        target_columns = json.loads(str(plan["target_columns_json"]))
        payload_digest = request_hash({"old_id": old_id, "new_id": new_id})
        for database_key in DATABASE_ORDER:
            self._apply_database_step(
                database_key,
                operation_id,
                payload_digest,
                old_id,
                new_id,
                target_columns.get(database_key, []),
            )
            if database_key == "game_db":
                self.invalidate_user_id_cache()
            if database_key == "player_db":
                for table, column in target_columns.get(database_key, []):
                    self.invalidate_player_data_cache(str(table), (str(column),))
            self._after_database_step(database_key)
            self._record_database_complete(operation_id, database_key)
        self._rename_player_directory(plan)
        total = 0
        for database_key in DATABASE_ORDER:
            with DatabaseUnitOfWork(self.databases[database_key], read_only=True) as uow:
                row = uow.query_one(
                    "SELECT updated_cells FROM admin_id_update_step_receipts WHERE operation_id=?",
                    (operation_id,),
                )
            if row is None:
                raise RuntimeError(f"{database_key} 缺少 ID更新步骤回执")
            total += int(row["updated_cells"])
        message = f"手动ID更新完成：{old_id} -> {new_id}\n总更新单元格：{total}\n玩家目录：{self._directory_message(plan)}"
        outcome = OperationOutcome.applied(
            operation_id,
            ACTION,
            data={"old_id": old_id, "new_id": new_id, "updated_cells": total},
            message=message,
            audit_category="admin",
        )
        with DatabaseUnitOfWork(self.databases["game_db"], immediate=True) as uow:
            uow.execute(
                "UPDATE admin_id_update_operations SET status='applied',directory_phase='completed',"
                "last_error='',updated_at=? WHERE operation_id=?",
                (_now(), operation_id),
            )
            self.ledger.finish(uow, outcome)
            self.outbox.mark_sent(uow, f"{operation_id}:{ACTION}")
        return outcome

    def _apply_database_step(
        self,
        database_key: str,
        operation_id: str,
        payload_digest: str,
        old_id: str,
        new_id: str,
        raw_targets: list[list[str]],
    ) -> int:
        targets = [(str(table), str(column)) for table, column in raw_targets]
        with DatabaseUnitOfWork(
            self.databases[database_key], immediate=True, foreign_keys=False
        ) as uow:
            uow.execute("PRAGMA cache_size=-2048")
            uow.execute("PRAGMA temp_store=FILE")
            if not self._schema_ready(uow, database_key):
                raise _SchemaMissing(f"{database_key} ID更新迁移尚未就绪")
            receipt = uow.query_one(
                "SELECT payload_hash,updated_cells FROM admin_id_update_step_receipts WHERE operation_id=?",
                (operation_id,),
            )
            if receipt is not None:
                if str(receipt["payload_hash"]) != payload_digest:
                    raise OperationConflictError(operation_id, ACTION)
                return int(receipt["updated_cells"])
            updated_cells = 0
            for table, column in targets:
                target_exists = uow.query_one(
                    f"SELECT 1 AS present FROM {_quote_ident(table)} "
                    f"WHERE CAST({_quote_ident(column)} AS TEXT)=? LIMIT 1",
                    (new_id,),
                )
                if target_exists is not None:
                    raise RuntimeError(f"{database_key}.{table}.{column} 新 ID 在计划后被占用")
                cursor = uow.execute(
                    f"UPDATE {_quote_ident(table)} SET {_quote_ident(column)}=? "
                    f"WHERE CAST({_quote_ident(column)} AS TEXT)=?",
                    (new_id, old_id),
                )
                updated_cells += max(0, int(cursor.rowcount))
            violation = uow.query_one("PRAGMA foreign_key_check")
            if violation is not None:
                raise RuntimeError(f"{database_key} foreign key check failed for {violation['table']}")
            # The receipt and all column updates commit in this same database transaction.
            uow.execute(
                "INSERT INTO admin_id_update_step_receipts "
                "(operation_id,payload_hash,updated_cells,created_at) VALUES (?,?,?,?)",
                (operation_id, payload_digest, updated_cells, _now()),
            )
            return updated_cells

    def _after_database_step(self, database_key: str) -> None:
        """Failure-injection boundary for recovery tests."""

    def _record_database_complete(self, operation_id: str, database_key: str) -> None:
        with DatabaseUnitOfWork(self.databases["game_db"], immediate=True) as uow:
            row = uow.query_one(
                "SELECT completed_databases FROM admin_id_update_operations WHERE operation_id=?",
                (operation_id,),
            )
            if row is None:
                raise RuntimeError("ID更新计划丢失")
            completed = json.loads(str(row["completed_databases"]))
            if database_key not in completed:
                completed.append(database_key)
            uow.execute(
                "UPDATE admin_id_update_operations SET completed_databases=?,last_error='',updated_at=? "
                "WHERE operation_id=?",
                (json.dumps(completed, separators=(",", ":")), _now(), operation_id),
            )

    def _rename_player_directory(self, plan: Mapping[str, Any]) -> None:
        operation_id = str(plan["operation_id"])
        old_path = self.players_dir / str(plan["old_id"])
        new_path = self.players_dir / str(plan["new_id"])
        with DatabaseUnitOfWork(self.databases["game_db"], immediate=True) as uow:
            row = uow.query_one(
                "SELECT directory_phase,directory_old_present FROM admin_id_update_operations WHERE operation_id=?",
                (operation_id,),
            )
            if row is None:
                raise RuntimeError("ID更新目录恢复计划丢失")
            if str(row["directory_phase"]) == "completed":
                return
            old_present, new_present = _entry_exists(old_path), _entry_exists(new_path)
            if bool(row["directory_old_present"]):
                if old_present and not new_present:
                    self._rename_directory(old_path, new_path)
                    self._fsync_players_directory()
                elif old_present or not new_present:
                    raise RuntimeError("players 目录状态与ID更新计划冲突")
            elif old_present:
                raise RuntimeError("ID更新计划没有旧玩家目录，但旧目录当前存在")
            elif new_present:
                raise RuntimeError("ID更新计划没有新玩家目录，但目标目录当前被占用")
            uow.execute(
                "UPDATE admin_id_update_operations SET directory_phase='completed',updated_at=? "
                "WHERE operation_id=?",
                (_now(), operation_id),
            )

    def _rename_directory(self, source: Path, target: Path) -> None:
        source.rename(target)

    @staticmethod
    def _directory_message(plan: Mapping[str, Any]) -> str:
        return "已重命名" if bool(plan["directory_old_present"]) else "旧玩家目录不存在，跳过"

    def _fsync_players_directory(self) -> None:
        if os.name == "nt" or not self.players_dir.exists():
            return
        descriptor = os.open(self.players_dir, os.O_RDONLY | getattr(os, "O_DIRECTORY", 0))
        try:
            os.fsync(descriptor)
        finally:
            os.close(descriptor)

    def _mark_needs_reconcile(
        self, plan: Mapping[str, Any], error: BaseException
    ) -> OperationOutcome[dict[str, Any]]:
        operation_id = str(plan["operation_id"])
        message = (
            f"ID更新未完全结束，已记录恢复任务：{str(error)[:500]}。"
            "请勿使用新操作号重复更新；启动恢复或重试同一操作会继续处理。"
        )
        outcome = OperationOutcome.failed(
            operation_id,
            ACTION,
            message,
            code="needs_reconcile",
            data={"completed_databases": json.loads(str(plan.get("completed_databases", "[]")))},
            audit_category="admin.id_update.reconcile",
        )
        with DatabaseUnitOfWork(self.databases["game_db"], immediate=True) as uow:
            uow.execute(
                "UPDATE admin_id_update_operations SET status='needs_reconcile',last_error=?,updated_at=? "
                "WHERE operation_id=? AND status<>'applied'",
                (str(error)[:1000], _now(), operation_id),
            )
            self.ledger.finish(uow, outcome)
            self.outbox.mark_failed(uow, f"{operation_id}:{ACTION}")
        return outcome

    def reconcile_pending(self, *, limit: int = 100) -> dict[str, int]:
        if not all(self.databases[key].is_file() for key in DATABASE_ORDER):
            return {"recovered": 0, "pending": 0, "failed": 0}
        with exclusive_id_mutation_lock(self.databases["game_db"]):
            with DatabaseUnitOfWork(self.databases["game_db"], read_only=True) as uow:
                if not self._schema_ready(uow, "game_db"):
                    return {"recovered": 0, "pending": 0, "failed": 0}
                plans = uow.query_all(
                    "SELECT * FROM admin_id_update_operations "
                    "WHERE status IN ('started','needs_reconcile') ORDER BY created_at LIMIT ?",
                    (max(1, min(int(limit), 100)),),
                )
                total_pending = int(
                    uow.query_one(
                        "SELECT COUNT(*) AS count FROM admin_id_update_operations "
                        "WHERE status IN ('started','needs_reconcile')"
                    )["count"]
                )
            if not plans:
                return {"recovered": 0, "pending": 0, "failed": 0}
            try:
                self._preflight_write_capacity()
            except OSError:
                return {"recovered": 0, "pending": total_pending, "failed": len(plans)}
            recovered = failed = 0
            for plan in plans:
                try:
                    self._resume(plan)
                    recovered += 1
                except Exception as exc:
                    self._mark_needs_reconcile(plan, exc)
                    failed += 1
            with DatabaseUnitOfWork(self.databases["game_db"], read_only=True) as uow:
                pending = int(
                    uow.query_one(
                        "SELECT COUNT(*) AS count FROM admin_id_update_operations "
                        "WHERE status IN ('started','needs_reconcile')"
                    )["count"]
                )
        return {"recovered": recovered, "pending": pending, "failed": failed}


__all__ = ["ACTION", "AdminIdUpdateSqlRepository"]
