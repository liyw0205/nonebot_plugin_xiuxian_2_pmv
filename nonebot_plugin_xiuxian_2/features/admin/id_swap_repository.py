from __future__ import annotations

import json
import os
from contextlib import contextmanager
from datetime import datetime, timezone
from pathlib import Path
import threading
import uuid
from typing import Any, Callable, Iterator, Mapping

from ...core.errors import OperationConflictError
from ...core.result import OperationOutcome
from ...infrastructure.database.backup_capacity import preflight_capacity
from ...infrastructure.database import DatabaseUnitOfWork, OperationLedger, OutboxStore
from ...infrastructure.database.ledger import request_hash


ACTION = "admin.id-swap"
DATABASE_ORDER = ("game_db", "impart_db", "trade_db", "player_db")
TARGET_COLUMNS = {
    "game_db": frozenset({"user_id", "sect_owner"}),
    "impart_db": frozenset({"user_id"}),
    "trade_db": frozenset({"user_id"}),
    "player_db": frozenset({"user_id", "partner_id", "group_id", "main_id", "active_id"}),
}
MAX_ID_BYTES = 255
_operation_lock = threading.RLock()


def _quote_ident(value: str) -> str:
    return '"' + value.replace('"', '""') + '"'


def _now() -> str:
    return datetime.now(timezone.utc).isoformat()


def _path_entry_exists(path: Path) -> bool:
    return path.exists() or path.is_symlink()


@contextmanager
def _exclusive_swap_lock(game_database: Path) -> Iterator[None]:
    """Serialize ID swaps across bot processes without holding a SQLite writer lock."""
    with _operation_lock:
        lock_path = game_database.with_name(".admin-id-swap.lock")
        with lock_path.open("a+b") as handle:
            if os.name == "nt":
                import msvcrt

                handle.seek(0, os.SEEK_END)
                if handle.tell() == 0:
                    handle.write(b"0")
                    handle.flush()
                handle.seek(0)
                msvcrt.locking(handle.fileno(), msvcrt.LK_LOCK, 1)
            else:
                import fcntl

                fcntl.flock(handle.fileno(), fcntl.LOCK_EX)
            try:
                yield
            finally:
                if os.name == "nt":
                    import msvcrt

                    handle.seek(0)
                    msvcrt.locking(handle.fileno(), msvcrt.LK_UNLCK, 1)
                else:
                    import fcntl

                    fcntl.flock(handle.fileno(), fcntl.LOCK_UN)


class AdminIdSwapSqlRepository:
    """Recoverable ID swaps across the legacy SQLite files and player folders."""

    def __init__(
        self,
        databases: Mapping[str, str | Path],
        players_dir: str | Path,
        *,
        invalidate_user_id_cache: Callable[[], Any] | None = None,
    ) -> None:
        self.databases = {key: Path(databases[key]) for key in DATABASE_ORDER}
        self.players_dir = Path(players_dir)
        self.invalidate_user_id_cache = invalidate_user_id_cache or (lambda: None)
        self.ledger = OperationLedger()
        self.outbox = OutboxStore()

    def swap(self, operation_id: str, id1: str, id2: str) -> OperationOutcome[dict[str, Any]]:
        operation_id = str(operation_id).strip()
        id1, id2 = str(id1).strip(), str(id2).strip()
        if not operation_id:
            return OperationOutcome.rejected("", ACTION, "operation_id 不能为空", code="invalid_operation")
        if not all(self.databases[key].is_file() for key in DATABASE_ORDER):
            return OperationOutcome.failed(
                operation_id,
                ACTION,
                "ID交换迁移尚未就绪：缺少一个或多个数据库文件。",
                code="schema_missing",
                audit_category="admin",
            )

        with _exclusive_swap_lock(self.databases["game_db"]):
            try:
                plan, terminal = self._prepare(operation_id, id1, id2)
            except Exception as exc:
                plan = self._load_plan(operation_id)
                if plan is not None:
                    return self._mark_needs_reconcile(plan, exc)
                return OperationOutcome.failed(
                    operation_id,
                    ACTION,
                    f"ID交换未执行：{exc}",
                    code="schema_or_storage_error",
                    audit_category="admin",
                )
            if terminal is not None:
                return terminal
            if plan is None:
                return OperationOutcome.failed(
                    operation_id,
                    ACTION,
                    "ID交换计划未能建立。",
                    code="operation_plan_missing",
                    audit_category="admin",
                )
            try:
                self._preflight_write_capacity()
            except OSError as exc:
                return OperationOutcome.failed(
                    operation_id,
                    ACTION,
                    f"磁盘容量不足或无法确认，ID交换尚未修改用户数据：{str(exc)[:500]}",
                    code=getattr(exc, "code", "storage_capacity_unavailable"),
                    audit_category="admin",
                )
            try:
                return self._resume(plan)
            except Exception as exc:
                return self._mark_needs_reconcile(plan, exc)

    def _schema_ready(self, uow: DatabaseUnitOfWork, database_key: str) -> bool:
        required = {"admin_id_swap_step_receipts"}
        if database_key == "game_db":
            required.update(
                {"admin_id_swap_operations", "operation_ledger", "domain_outbox"}
            )
        rows = uow.query_all(
            "SELECT name FROM sqlite_master WHERE type='table' AND name IN ("
            + ",".join("?" for _ in required)
            + ")",
            tuple(sorted(required)),
        )
        return {str(row["name"]) for row in rows} == required

    def _prepare(
        self, operation_id: str, id1: str, id2: str
    ) -> tuple[dict[str, Any] | None, OperationOutcome[dict[str, Any]] | None]:
        payload = {"id1": id1, "id2": id2}
        payload_digest = request_hash(payload)
        with DatabaseUnitOfWork(self.databases["game_db"], immediate=True) as game_uow:
            if not self._schema_ready(game_uow, "game_db"):
                return None, OperationOutcome.failed(
                    operation_id,
                    ACTION,
                    "ID交换迁移尚未就绪：请先完成启动迁移。",
                    code="schema_missing",
                    audit_category="admin",
                )

            previous = self.ledger.get(game_uow, operation_id, ACTION)
            try:
                self.ledger.begin(game_uow, operation_id, ACTION, payload)
            except OperationConflictError:
                return None, OperationOutcome.rejected(
                    operation_id,
                    ACTION,
                    "同一操作号对应了不同的 ID 参数，已拒绝重放。",
                    code="operation_payload_conflict",
                    audit_category="admin",
                )

            if previous is not None and previous.status in {"applied", "rejected"}:
                prior_outcome = previous.outcome()
                if prior_outcome is not None:
                    return None, prior_outcome.replay()
                return None, OperationOutcome.failed(
                    operation_id,
                    ACTION,
                    "ID交换操作回执缺少结果，需人工检查。",
                    code="operation_receipt_invalid",
                    audit_category="admin",
                )

            plan = game_uow.query_one(
                "SELECT * FROM admin_id_swap_operations WHERE operation_id=?",
                (operation_id,),
            )
            if plan is not None:
                if str(plan["payload_hash"]) != payload_digest:
                    return None, OperationOutcome.rejected(
                        operation_id,
                        ACTION,
                        "同一操作号对应了不同的 ID 参数，已拒绝重放。",
                        code="operation_payload_conflict",
                        audit_category="admin",
                    )
                self.outbox.append(
                    game_uow,
                    event_id=f"{operation_id}:{ACTION}",
                    aggregate_type="admin_id_swap",
                    aggregate_id=operation_id,
                    event_type=ACTION,
                    payload=payload,
                )
                return dict(plan), None

            self.outbox.append(
                game_uow,
                event_id=f"{operation_id}:{ACTION}",
                aggregate_type="admin_id_swap",
                aggregate_id=operation_id,
                event_type=ACTION,
                payload=payload,
            )

            rejection = self._validate_ids(id1, id2)
            if rejection is not None:
                return None, self._finish_rejected(game_uow, operation_id, rejection)

            pending = game_uow.query_one(
                "SELECT operation_id FROM admin_id_swap_operations "
                "WHERE status IN ('started','needs_reconcile') LIMIT 1"
            )
            if pending is not None:
                return None, self._finish_rejected(
                    game_uow,
                    operation_id,
                    (
                        "另一笔 ID交换仍待恢复："
                        f"{pending['operation_id']}。恢复完成前不能开始新交换。"
                    ),
                    code="reconcile_pending",
                )

            first_exists, second_exists = self._id_pair_exists(game_uow, id1, id2)
            if not first_exists or not second_exists:
                return None, self._finish_rejected(
                    game_uow,
                    operation_id,
                    f"交换失败：ID1存在={first_exists}，ID2存在={second_exists}。要求两者都存在。",
                    code="user_id_missing",
                )

            temp_id = self._allocate_temp_id(game_uow)
            if temp_id is None:
                return None, self._finish_rejected(
                    game_uow,
                    operation_id,
                    "无法分配无冲突的临时 ID，未执行交换。",
                    code="temporary_id_conflict",
                )

            id1_path, id2_path = self.players_dir / id1, self.players_dir / id2
            plan_values = {
                "operation_id": operation_id,
                "payload_hash": payload_digest,
                "id1": id1,
                "id2": id2,
                "temp_id": temp_id,
                "directory_id1_present": int(_path_entry_exists(id1_path)),
                "directory_id2_present": int(_path_entry_exists(id2_path)),
                "directory_phase": "pending",
                "status": "started",
                "completed_databases": "[]",
                "last_error": "",
                "created_at": _now(),
                "updated_at": _now(),
            }
            game_uow.execute(
                "INSERT INTO admin_id_swap_operations "
                "(operation_id,payload_hash,id1,id2,temp_id,directory_id1_present,"
                "directory_id2_present,directory_phase,status,completed_databases,last_error,created_at,updated_at) "
                "VALUES (:operation_id,:payload_hash,:id1,:id2,:temp_id,:directory_id1_present,"
                ":directory_id2_present,:directory_phase,:status,:completed_databases,:last_error,:created_at,:updated_at)",
                plan_values,
            )
            return plan_values, None

    def _preflight_write_capacity(self) -> None:
        requirements = {
            path: max(1, path.stat().st_size) * 3
            for path in set(self.databases.values())
        }
        preflight_capacity(requirements, operation="admin ID swap WAL workspace")

    @staticmethod
    def _validate_ids(id1: str, id2: str) -> str | None:
        if not id1 or not id2:
            return "参数错误：ID1 和 ID2 不能为空"
        if id1 == id2:
            return "ID1 与 ID2 相同，无法交换"
        for value in (id1, id2):
            if (
                value in {".", ".."}
                or "/" in value
                or "\\" in value
                or "\x00" in value
                or len(value.encode("utf-8")) > MAX_ID_BYTES
            ):
                return "ID格式不支持：ID必须是单个目录名且不能超过255个UTF-8字节。"
        return None

    def _finish_rejected(
        self,
        uow: DatabaseUnitOfWork,
        operation_id: str,
        message: str,
        *,
        code: str = "rejected",
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

    def _id_pair_exists(
        self, game_uow: DatabaseUnitOfWork, id1: str, id2: str
    ) -> tuple[bool, bool]:
        found = {id1: False, id2: False}
        for database_key in DATABASE_ORDER:
            if database_key == "game_db":
                self._update_found_ids(game_uow, database_key, found)
            else:
                with DatabaseUnitOfWork(
                    self.databases[database_key], read_only=True
                ) as uow:
                    if not self._schema_ready(uow, database_key):
                        raise RuntimeError(f"{database_key} ID交换迁移尚未就绪")
                    self._update_found_ids(uow, database_key, found)
            if all(found.values()):
                break
        return found[id1], found[id2]

    def _update_found_ids(
        self, uow: DatabaseUnitOfWork, database_key: str, found: dict[str, bool]
    ) -> None:
        for table, column in self._id_table_targets(uow, database_key):
            table_sql, column_sql = _quote_ident(table), _quote_ident(column)
            for value in found:
                if found[value]:
                    continue
                row = uow.query_one(
                    f"SELECT 1 AS present FROM {table_sql} "
                    f"WHERE CAST({column_sql} AS TEXT)=? LIMIT 1",
                    (value,),
                )
                found[value] = row is not None
            if all(found.values()):
                return

    @staticmethod
    def _id_table_targets(
        uow: DatabaseUnitOfWork, database_key: str
    ) -> Iterator[tuple[str, str]]:
        targets = TARGET_COLUMNS[database_key]
        tables = uow.query_all(
            "SELECT name FROM sqlite_master WHERE type='table' AND name NOT LIKE 'sqlite_%' ORDER BY name"
        )
        for row in tables:
            table = str(row["name"])
            table_sql = _quote_ident(table)
            columns = {
                str(column["name"])
                for column in uow.query_all(f"PRAGMA table_info({table_sql})")
            }
            for column in sorted(columns.intersection(targets)):
                yield table, column

    def _allocate_temp_id(self, game_uow: DatabaseUnitOfWork) -> str | None:
        for _ in range(4):
            candidate = f"__id_swap_{uuid.uuid4().hex}"
            if self._id_exists(game_uow, "game_db", candidate):
                continue
            collision = False
            for database_key in DATABASE_ORDER[1:]:
                with DatabaseUnitOfWork(
                    self.databases[database_key], read_only=True
                ) as uow:
                    if not self._schema_ready(uow, database_key):
                        raise RuntimeError(f"{database_key} ID交换迁移尚未就绪")
                    if self._id_exists(uow, database_key, candidate):
                        collision = True
                        break
            if not collision and not _path_entry_exists(self.players_dir / candidate):
                return candidate
        return None

    def _id_exists(self, uow: DatabaseUnitOfWork, database_key: str, value: str) -> bool:
        for table, column in self._id_table_targets(uow, database_key):
            row = uow.query_one(
                f"SELECT 1 AS present FROM {_quote_ident(table)} "
                f"WHERE CAST({_quote_ident(column)} AS TEXT)=? LIMIT 1",
                (value,),
            )
            if row is not None:
                return True
        return False

    def _load_plan(self, operation_id: str) -> dict[str, Any] | None:
        try:
            with DatabaseUnitOfWork(
                self.databases["game_db"], read_only=True
            ) as uow:
                row = uow.query_one(
                    "SELECT * FROM admin_id_swap_operations WHERE operation_id=?",
                    (operation_id,),
                )
            return dict(row) if row is not None else None
        except Exception:
            return None

    def _resume(self, plan: Mapping[str, Any]) -> OperationOutcome[dict[str, Any]]:
        operation_id = str(plan["operation_id"])
        receipt_hash = request_hash(
            {
                "operation_id": operation_id,
                "id1": str(plan["id1"]),
                "id2": str(plan["id2"]),
                "temp_id": str(plan["temp_id"]),
            }
        )
        for database_key in DATABASE_ORDER:
            updated = self._apply_database_step(
                database_key,
                operation_id,
                receipt_hash,
                str(plan["id1"]),
                str(plan["id2"]),
                str(plan["temp_id"]),
            )
            if database_key == "game_db":
                self.invalidate_user_id_cache()
            self._after_database_step(database_key)
            self._record_database_complete(operation_id, database_key)
        self._swap_directories(plan)

        total = 0
        for database_key in DATABASE_ORDER:
            with DatabaseUnitOfWork(
                self.databases[database_key], read_only=True
            ) as uow:
                row = uow.query_one(
                    "SELECT updated_cells FROM admin_id_swap_step_receipts WHERE operation_id=?",
                    (operation_id,),
                )
            if row is None:
                raise RuntimeError(f"{database_key} 缺少 ID交换步骤回执")
            total += int(row["updated_cells"])

        id1, id2 = str(plan["id1"]), str(plan["id2"])
        directory_message = "已完成交换" if (
            bool(plan["directory_id1_present"]) or bool(plan["directory_id2_present"])
        ) else "无玩家目录需要交换"
        message = (
            f"ID交换完成：{id1} - {id2}\n"
            f"总更新单元格：{total}\n"
            f"players目录：{directory_message}"
        )
        outcome = OperationOutcome.applied(
            operation_id,
            ACTION,
            data={
                "id1": id1,
                "id2": id2,
                "updated_cells": total,
                "players_directory": directory_message,
            },
            message=message,
            audit_category="admin",
        )
        with DatabaseUnitOfWork(
            self.databases["game_db"], immediate=True
        ) as uow:
            uow.execute(
                "UPDATE admin_id_swap_operations SET status='applied',directory_phase='completed',"
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
        payload_hash: str,
        id1: str,
        id2: str,
        temp_id: str,
    ) -> int:
        if not self.databases[database_key].is_file():
            raise RuntimeError(f"{database_key} 数据库文件不存在")
        with DatabaseUnitOfWork(
            self.databases[database_key], immediate=True, foreign_keys=False
        ) as uow:
            uow.execute("PRAGMA cache_size=-2048")
            uow.execute("PRAGMA temp_store=FILE")
            if not self._schema_ready(uow, database_key):
                raise RuntimeError(f"{database_key} ID交换迁移尚未就绪")
            receipt = uow.query_one(
                "SELECT payload_hash,updated_cells FROM admin_id_swap_step_receipts WHERE operation_id=?",
                (operation_id,),
            )
            if receipt is not None:
                if str(receipt["payload_hash"]) != payload_hash:
                    raise OperationConflictError(operation_id, ACTION)
                return int(receipt["updated_cells"])

            targets = list(self._id_table_targets(uow, database_key))
            for table, column in targets:
                if uow.query_one(
                    f"SELECT 1 AS present FROM {_quote_ident(table)} "
                    f"WHERE CAST({_quote_ident(column)} AS TEXT)=? LIMIT 1",
                    (temp_id,),
                ):
                    raise RuntimeError(f"{database_key} 临时 ID 已存在，拒绝合并用户记录")

            updated_cells = 0
            for old_id, new_id in ((id1, temp_id), (id2, id1), (temp_id, id2)):
                for table, column in targets:
                    cursor = uow.execute(
                        f"UPDATE {_quote_ident(table)} SET {_quote_ident(column)}=? "
                        f"WHERE CAST({_quote_ident(column)} AS TEXT)=?",
                        (new_id, old_id),
                    )
                    updated_cells += max(0, int(cursor.rowcount))
            uow.execute(
                "INSERT INTO admin_id_swap_step_receipts "
                "(operation_id,payload_hash,updated_cells,created_at) VALUES (?,?,?,?)",
                (operation_id, payload_hash, updated_cells, _now()),
            )
            return updated_cells

    def _after_database_step(self, database_key: str) -> None:
        """Failure-injection boundary for recovery tests."""

    def _record_database_complete(self, operation_id: str, database_key: str) -> None:
        with DatabaseUnitOfWork(
            self.databases["game_db"], immediate=True
        ) as uow:
            row = uow.query_one(
                "SELECT completed_databases FROM admin_id_swap_operations WHERE operation_id=?",
                (operation_id,),
            )
            if row is None:
                raise RuntimeError("ID交换计划丢失")
            completed = json.loads(str(row["completed_databases"]))
            if database_key not in completed:
                completed.append(database_key)
            uow.execute(
                "UPDATE admin_id_swap_operations SET completed_databases=?,last_error='',updated_at=? "
                "WHERE operation_id=?",
                (json.dumps(completed, separators=(",", ":")), _now(), operation_id),
            )

    def _directory_state(self, paths: tuple[Path, Path, Path]) -> tuple[bool, bool, bool]:
        return tuple(_path_entry_exists(path) for path in paths)  # type: ignore[return-value]

    def _swap_directories(self, plan: Mapping[str, Any]) -> None:
        id1, id2, temp_id = (str(plan[key]) for key in ("id1", "id2", "temp_id"))
        paths = (self.players_dir / id1, self.players_dir / id2, self.players_dir / temp_id)
        initial = (bool(plan["directory_id1_present"]), bool(plan["directory_id2_present"]), False)
        after_first = (False, initial[1], initial[0])
        after_second = (initial[1], False, initial[0])
        completed = (initial[1], initial[0], False)
        phases = (
            ("pending", "first_moved", initial, after_first, 0, 2, initial[0]),
            ("first_moved", "second_moved", after_first, after_second, 1, 0, initial[1]),
            ("second_moved", "completed", after_second, completed, 2, 1, initial[0]),
        )
        for phase, next_phase, before, after, source_index, target_index, should_move in phases:
            with DatabaseUnitOfWork(
                self.databases["game_db"], immediate=True
            ) as uow:
                row = uow.query_one(
                    "SELECT directory_phase FROM admin_id_swap_operations WHERE operation_id=?",
                    (str(plan["operation_id"]),),
                )
                if row is None:
                    raise RuntimeError("ID交换目录恢复计划丢失")
                current_phase = str(row["directory_phase"])
                if current_phase == "completed":
                    return
                if current_phase != phase:
                    continue
                state = self._directory_state(paths)
                if state == before:
                    if should_move:
                        source, target = paths[source_index], paths[target_index]
                        if not _path_entry_exists(source) or _path_entry_exists(target):
                            raise RuntimeError("players 目录状态与交换计划冲突")
                        self._rename_directory(source, target)
                        self._fsync_players_directory()
                elif state != after:
                    raise RuntimeError(
                        f"players 目录状态无法恢复：phase={phase}, state={state}"
                    )
                uow.execute(
                    "UPDATE admin_id_swap_operations SET directory_phase=?,updated_at=? "
                    "WHERE operation_id=?",
                    (next_phase, _now(), str(plan["operation_id"])),
                )

    def _rename_directory(self, source: Path, target: Path) -> None:
        source.rename(target)

    def _fsync_players_directory(self) -> None:
        if os.name == "nt" or not self.players_dir.exists():
            return
        descriptor = os.open(
            self.players_dir,
            os.O_RDONLY | getattr(os, "O_DIRECTORY", 0),
        )
        try:
            os.fsync(descriptor)
        finally:
            os.close(descriptor)

    def _mark_needs_reconcile(
        self, plan: Mapping[str, Any], error: BaseException
    ) -> OperationOutcome[dict[str, Any]]:
        operation_id = str(plan["operation_id"])
        message = (
            f"ID交换未完全结束，已记录恢复任务：{str(error)[:500]}。"
            "请勿用新操作号重复交换；启动恢复或重试同一操作会继续处理。"
        )
        outcome = OperationOutcome.failed(
            operation_id,
            ACTION,
            message,
            code="needs_reconcile",
            data={"completed_databases": json.loads(str(plan.get("completed_databases", "[]")))},
            audit_category="admin.id_swap.reconcile",
        )
        with DatabaseUnitOfWork(
            self.databases["game_db"], immediate=True
        ) as uow:
            uow.execute(
                "UPDATE admin_id_swap_operations SET status='needs_reconcile',last_error=?,updated_at=? "
                "WHERE operation_id=? AND status<>'applied'",
                (str(error)[:1000], _now(), operation_id),
            )
            self.ledger.finish(uow, outcome)
            self.outbox.mark_failed(uow, f"{operation_id}:{ACTION}")
        return outcome

    def reconcile_pending(self, *, limit: int = 100) -> dict[str, int]:
        if not all(self.databases[key].is_file() for key in DATABASE_ORDER):
            return {"recovered": 0, "pending": 0, "failed": 0}
        with _exclusive_swap_lock(self.databases["game_db"]):
            with DatabaseUnitOfWork(
                self.databases["game_db"], read_only=True
            ) as uow:
                if not self._schema_ready(uow, "game_db"):
                    return {"recovered": 0, "pending": 0, "failed": 0}
                plans = uow.query_all(
                    "SELECT * FROM admin_id_swap_operations "
                    "WHERE status IN ('started','needs_reconcile') ORDER BY created_at LIMIT ?",
                    (max(1, min(int(limit), 100)),),
                )
                total_pending = int(
                    uow.query_one(
                        "SELECT COUNT(*) AS count FROM admin_id_swap_operations "
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
            with DatabaseUnitOfWork(
                self.databases["game_db"], read_only=True
            ) as uow:
                pending = int(
                    uow.query_one(
                        "SELECT COUNT(*) AS count FROM admin_id_swap_operations "
                        "WHERE status IN ('started','needs_reconcile')"
                    )["count"]
                )
        return {"recovered": recovered, "pending": pending, "failed": failed}


__all__ = ["ACTION", "AdminIdSwapSqlRepository"]
