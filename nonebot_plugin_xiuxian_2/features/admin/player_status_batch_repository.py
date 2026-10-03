from __future__ import annotations

import json
import shutil
from dataclasses import dataclass
from pathlib import Path
from threading import RLock
from typing import Any

from ...core.numeric import as_int_like
from ...infrastructure.database import DatabaseUnitOfWork


@dataclass(frozen=True)
class AdminPlayerStatusBatchResetResult:
    status: str
    total: int = 0
    completed: int = 0
    reset_users: int = 0
    skipped_users: int = 0

    @property
    def succeeded(self) -> bool:
        return self.status in {"applied", "duplicate"}


@dataclass(frozen=True)
class _PlayerResetResult:
    status: str
    previous_state: tuple[int, int, int, int, int] = ()
    final_state: tuple[int, int, int, int, int] = ()

    @property
    def succeeded(self) -> bool:
        return self.status in {"reset", "duplicate"}


class AdminPlayerStatusBatchResetSqlRepository:
    """Freeze a reset target set in SQL and process it in bounded chunks."""

    MAX_CHUNK_SIZE = 100
    _OPERATION_COLUMNS = {
        "operation_id", "payload", "total", "status", "operator_id", "max_stamina"
    }
    _PROGRESS_COLUMNS = {
        "operation_id", "user_id", "status", "reset_applied", "result_json"
    }
    _TARGET_COLUMNS = {"operation_id", "user_id", "ordinal"}
    _RESET_COLUMNS = {
        "operation_id", "payload", "previous_state", "final_state"
    }
    _PLAYER_COLUMNS = {"user_id", "exp", "hp", "mp", "atk", "user_stamina"}

    def __init__(self, database: str | Path, *, lock: RLock | None = None) -> None:
        self.database = Path(database)
        self.lock = lock or RLock()

    @staticmethod
    def _columns(uow: DatabaseUnitOfWork, table: str) -> set[str]:
        return {str(row["name"]) for row in uow.query_all(f'PRAGMA table_info("{table}")')}

    @classmethod
    def _schema_ready(cls, uow: DatabaseUnitOfWork) -> bool:
        tables = {
            str(row["name"])
            for row in uow.query_all("SELECT name FROM sqlite_master WHERE type='table'")
        }
        required_tables = {
            "user_xiuxian",
            "admin_player_status_reset_operations",
            "admin_player_status_batch_reset_operations",
            "admin_player_status_batch_reset_progress",
            "admin_player_status_batch_reset_targets",
        }
        if not required_tables.issubset(tables):
            return False
        return (
            cls._PLAYER_COLUMNS.issubset(cls._columns(uow, "user_xiuxian"))
            and cls._RESET_COLUMNS.issubset(
                cls._columns(uow, "admin_player_status_reset_operations")
            )
            and cls._OPERATION_COLUMNS.issubset(
                cls._columns(uow, "admin_player_status_batch_reset_operations")
            )
            and cls._PROGRESS_COLUMNS.issubset(
                cls._columns(uow, "admin_player_status_batch_reset_progress")
            )
            and cls._TARGET_COLUMNS.issubset(
                cls._columns(uow, "admin_player_status_batch_reset_targets")
            )
        )

    @staticmethod
    def _request(operator_id: Any, max_stamina: Any) -> dict[str, Any]:
        return {"operator_id": str(operator_id).strip(), "max_stamina": int(max_stamina)}

    @classmethod
    def _result(
        cls, uow: DatabaseUnitOfWork, operation_id: str, status: str
    ) -> AdminPlayerStatusBatchResetResult:
        operation = uow.query_one(
            "SELECT total FROM admin_player_status_batch_reset_operations "
            "WHERE operation_id=?",
            (operation_id,),
        )
        if operation is None:
            return AdminPlayerStatusBatchResetResult(status)
        counts = uow.query_one(
            "SELECT COUNT(*) AS completed,COALESCE(SUM(reset_applied),0) AS reset_users "
            "FROM admin_player_status_batch_reset_progress WHERE operation_id=?",
            (operation_id,),
        )
        completed = int(counts["completed"])
        reset_users = int(counts["reset_users"])
        return AdminPlayerStatusBatchResetResult(
            status,
            int(operation["total"]),
            completed,
            reset_users,
            completed - reset_users,
        )

    def _has_target_capacity(self, uow: DatabaseUnitOfWork) -> bool:
        uow.execute("PRAGMA cache_size=-2048")
        uow.execute("PRAGMA temp_store=FILE")
        size = uow.query_one(
            "SELECT COUNT(*) AS target_count,COALESCE(SUM(length(CAST(user_id AS BLOB))),0) AS id_bytes "
            "FROM (SELECT trim(CAST(user_id AS TEXT)) AS user_id FROM user_xiuxian "
            "WHERE user_id IS NOT NULL AND trim(CAST(user_id AS TEXT))<>'' "
            "GROUP BY trim(CAST(user_id AS TEXT)))"
        )
        required = max(
            1024 * 1024,
            int(size["target_count"]) * 256 + int(size["id_bytes"]) * 4,
        )
        return shutil.disk_usage(self.database.parent).free >= required

    def find_running(self, operator_id: Any, max_stamina: Any) -> str | None:
        if not self.database.is_file():
            return None
        request = self._request(operator_id, max_stamina)
        with self.lock, DatabaseUnitOfWork(self.database, read_only=True) as uow:
            if not self._schema_ready(uow):
                return None
            row = uow.query_one(
                "SELECT operation_id FROM admin_player_status_batch_reset_operations "
                "WHERE status='running' AND operator_id=? AND max_stamina=? "
                "ORDER BY created_at DESC,rowid DESC LIMIT 1",
                (request["operator_id"], request["max_stamina"]),
            )
        return str(row["operation_id"]) if row is not None else None

    def _begin(
        self,
        operation_id: str,
        request: dict[str, Any],
        chunk_size: int,
    ) -> tuple[AdminPlayerStatusBatchResetResult | None, tuple[str, ...]]:
        with self.lock, DatabaseUnitOfWork(self.database, immediate=True) as uow:
            if not self._schema_ready(uow):
                return AdminPlayerStatusBatchResetResult("schema_missing"), ()

            operation = uow.query_one(
                "SELECT operator_id,max_stamina,status FROM "
                "admin_player_status_batch_reset_operations WHERE operation_id=?",
                (operation_id,),
            )
            if operation is None:
                if not self._has_target_capacity(uow):
                    return AdminPlayerStatusBatchResetResult("insufficient_storage"), ()
                payload = json.dumps(
                    {"request": request},
                    ensure_ascii=True,
                    sort_keys=True,
                    separators=(",", ":"),
                )
                uow.execute(
                    "INSERT INTO admin_player_status_batch_reset_operations"
                    "(operation_id,payload,total,status,operator_id,max_stamina) "
                    "VALUES(?,?,0,'running',?,?)",
                    (operation_id, payload, request["operator_id"], request["max_stamina"]),
                )
                uow.execute(
                    "INSERT INTO admin_player_status_batch_reset_targets"
                    "(operation_id,user_id,ordinal) "
                    "SELECT ?,user_id,ROW_NUMBER() OVER (ORDER BY user_id)-1 FROM ("
                    "SELECT DISTINCT trim(CAST(user_id AS TEXT)) AS user_id "
                    "FROM user_xiuxian WHERE user_id IS NOT NULL "
                    "AND trim(CAST(user_id AS TEXT))<>'')",
                    (operation_id,),
                )
                total = uow.query_one(
                    "SELECT COUNT(*) AS total FROM "
                    "admin_player_status_batch_reset_targets WHERE operation_id=?",
                    (operation_id,),
                )
                uow.execute(
                    "UPDATE admin_player_status_batch_reset_operations SET total=?,"
                    "status=CASE WHEN ?=0 THEN 'completed' ELSE 'running' END,"
                    "updated_at=CURRENT_TIMESTAMP WHERE operation_id=?",
                    (int(total["total"]), int(total["total"]), operation_id),
                )
                if int(total["total"]) == 0:
                    return self._result(uow, operation_id, "applied"), ()
            elif (
                str(operation["operator_id"]) != request["operator_id"]
                or int(operation["max_stamina"]) != request["max_stamina"]
            ):
                return self._result(uow, operation_id, "operation_conflict"), ()
            elif str(operation["status"]) == "completed":
                return self._result(uow, operation_id, "duplicate"), ()

            pending = uow.query_all(
                "SELECT t.user_id FROM admin_player_status_batch_reset_targets t "
                "WHERE t.operation_id=? AND NOT EXISTS("
                "SELECT 1 FROM admin_player_status_batch_reset_progress p "
                "WHERE p.operation_id=t.operation_id AND p.user_id=t.user_id) "
                "ORDER BY t.ordinal LIMIT ?",
                (operation_id, chunk_size),
            )
            if not pending:
                uow.execute(
                    "UPDATE admin_player_status_batch_reset_operations "
                    "SET status='completed',updated_at=CURRENT_TIMESTAMP "
                    "WHERE operation_id=?",
                    (operation_id,),
                )
                return self._result(uow, operation_id, "applied"), ()
            return None, tuple(str(row["user_id"]) for row in pending)

    @staticmethod
    def _state(row: Any) -> tuple[int, int, int, int, int]:
        return tuple(as_int_like(value, 0) for value in row)

    @staticmethod
    def _number_count(value: int) -> int | str:
        if value > 2**63 - 1 or value < -(2**63):
            return str(value)
        return value

    def _reset_one(
        self,
        operation_id: str,
        operator_id: str,
        user_id: str,
        expected_state: tuple[int, int, int, int, int] | None,
        max_stamina: int,
        *,
        force: bool,
    ) -> _PlayerResetResult:
        payload = json.dumps(
            [operator_id, user_id, max_stamina, int(force)],
            ensure_ascii=True,
            separators=(",", ":"),
        )
        with self.lock, DatabaseUnitOfWork(self.database, immediate=True) as uow:
            previous = uow.query_one(
                "SELECT payload,previous_state,final_state "
                "FROM admin_player_status_reset_operations WHERE operation_id=?",
                (operation_id,),
            )
            if previous is not None:
                try:
                    previous_request = json.loads(str(previous["payload"]))
                except (TypeError, ValueError, json.JSONDecodeError):
                    previous_request = None
                if not (
                    isinstance(previous_request, list)
                    and len(previous_request) == 4
                    and previous_request[:3] == [operator_id, user_id, max_stamina]
                    and previous_request[3] in (0, 1)
                ):
                    return _PlayerResetResult("operation_conflict")
                return _PlayerResetResult(
                    "duplicate",
                    tuple(json.loads(str(previous["previous_state"]))),
                    tuple(json.loads(str(previous["final_state"]))),
                )

            row = uow.query_one(
                "SELECT exp,hp,mp,atk,user_stamina FROM user_xiuxian WHERE user_id=?",
                (user_id,),
            )
            if row is None:
                return _PlayerResetResult("user_missing")
            actual_state = self._state(tuple(row[field] for field in (
                "exp", "hp", "mp", "atk", "user_stamina"
            )))
            if not force and expected_state is not None and actual_state != expected_state:
                return _PlayerResetResult("state_changed", actual_state, actual_state)

            final_state = (
                actual_state[0],
                actual_state[0] // 2,
                actual_state[0],
                actual_state[0] // 10,
                max_stamina,
            )
            changed = uow.execute(
                "UPDATE user_xiuxian SET hp=CAST(? AS REAL),mp=CAST(? AS REAL),"
                "atk=CAST(? AS REAL),user_stamina=? WHERE user_id=?",
                (
                    self._number_count(final_state[1]),
                    self._number_count(final_state[2]),
                    self._number_count(final_state[3]),
                    max_stamina,
                    user_id,
                ),
            )
            if changed.rowcount != 1:
                return _PlayerResetResult("state_changed", actual_state, actual_state)
            uow.execute(
                "INSERT INTO admin_player_status_reset_operations"
                "(operation_id,payload,previous_state,final_state) VALUES(?,?,?,?)",
                (
                    operation_id,
                    payload,
                    json.dumps(
                        [self._number_count(value) if index < 4 else value
                         for index, value in enumerate(actual_state)],
                        ensure_ascii=True,
                        separators=(",", ":"),
                    ),
                    json.dumps(
                        [self._number_count(value) if index < 4 else value
                         for index, value in enumerate(final_state)],
                        ensure_ascii=True,
                        separators=(",", ":"),
                    ),
                ),
            )
            return _PlayerResetResult("reset", actual_state, final_state)

    def _record(
        self, operation_id: str, user_id: str, result: _PlayerResetResult
    ) -> None:
        result_json = json.dumps(
            {
                "status": result.status,
                "previous_state": result.previous_state,
                "final_state": result.final_state,
            },
            ensure_ascii=True,
            sort_keys=True,
            separators=(",", ":"),
        )
        with self.lock, DatabaseUnitOfWork(self.database, immediate=True) as uow:
            uow.execute(
                "INSERT INTO admin_player_status_batch_reset_progress"
                "(operation_id,user_id,status,reset_applied,result_json) "
                "VALUES(?,?,?,?,?) ON CONFLICT(operation_id,user_id) DO NOTHING",
                (operation_id, user_id, result.status, int(result.succeeded), result_json),
            )
            counts = uow.query_one(
                "SELECT o.total,COUNT(p.user_id) AS completed "
                "FROM admin_player_status_batch_reset_operations o "
                "LEFT JOIN admin_player_status_batch_reset_progress p "
                "ON p.operation_id=o.operation_id WHERE o.operation_id=? "
                "GROUP BY o.total",
                (operation_id,),
            )
            status = "completed" if int(counts["completed"]) >= int(counts["total"]) else "running"
            uow.execute(
                "UPDATE admin_player_status_batch_reset_operations SET status=?,"
                "updated_at=CURRENT_TIMESTAMP WHERE operation_id=?",
                (status, operation_id),
            )

    def reset(
        self,
        operation_id: Any,
        operator_id: Any,
        max_stamina: Any,
        *,
        chunk_size: int = MAX_CHUNK_SIZE,
    ) -> AdminPlayerStatusBatchResetResult:
        operation_id = str(operation_id).strip()
        request = self._request(operator_id, max_stamina)
        if not operation_id or not request["operator_id"] or request["max_stamina"] <= 0:
            raise ValueError("invalid player status batch reset arguments")
        chunk_size = min(self.MAX_CHUNK_SIZE, max(1, int(chunk_size)))
        if not self.database.is_file():
            return AdminPlayerStatusBatchResetResult("schema_missing")

        previous, pending = self._begin(operation_id, request, chunk_size)
        if previous is not None:
            return previous

        for user_id in pending:
            result: _PlayerResetResult | None = None
            for attempt in range(4):
                with DatabaseUnitOfWork(self.database, read_only=True) as uow:
                    row = uow.query_one(
                        "SELECT exp,hp,mp,atk,user_stamina FROM user_xiuxian WHERE user_id=?",
                        (user_id,),
                    )
                expected_state = self._state(tuple(row.values())) if row is not None else None
                result = self._reset_one(
                    f"admin-player-status-reset-batch:{operation_id}:{user_id}",
                    request["operator_id"],
                    user_id,
                    expected_state,
                    request["max_stamina"],
                    force=attempt >= 2,
                )
                if result.status != "state_changed":
                    break
            if result is not None:
                self._record(operation_id, user_id, result)

        with self.lock, DatabaseUnitOfWork(self.database, read_only=True) as uow:
            return self._result(uow, operation_id, "applied")


__all__ = [
    "AdminPlayerStatusBatchResetResult",
    "AdminPlayerStatusBatchResetSqlRepository",
]
