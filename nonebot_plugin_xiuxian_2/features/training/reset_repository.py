from __future__ import annotations

import json
from dataclasses import dataclass
from datetime import date, datetime
from pathlib import Path
from threading import RLock
from typing import Any

from ...infrastructure.clock import SystemClock
from ...infrastructure.database.attached_uow import AttachedDatabaseUnitOfWork


@dataclass(frozen=True)
class TrainingResetResult:
    status: str
    task_status: str = ""
    reset_date: str = ""
    total: int = 0
    completed: int = 0
    changed: int = 0
    skipped: int = 0

    @property
    def succeeded(self) -> bool:
        return self.status in {"applied", "duplicate"}


class TrainingResetSqlRepository:
    """Feature-owned resumable reset of the frozen training player set."""

    _STATE_FIELDS = (
        "progress",
        "last_time",
        "points",
        "completed",
        "max_progress",
        "last_event",
        "weekly_purchases",
    )
    _GAME_TABLES = {
        "user_xiuxian",
        "admin_training_reset_operations",
        "admin_training_reset_targets",
    }
    _PLAYER_TABLES = {"training"}
    _OPERATION_COLUMNS = {
        "operation_id",
        "payload",
        "reset_date",
        "total",
        "completed",
        "changed",
        "skipped",
        "status",
        "created_at",
        "updated_at",
    }
    _TARGET_COLUMNS = {
        "operation_id",
        "user_id",
        "status",
        "previous_state",
        "updated_at",
    }

    def __init__(
        self,
        game_database: str | Path,
        player_database: str | Path,
        *,
        clock: Any | None = None,
        lock: RLock | None = None,
    ) -> None:
        self.game_database = str(game_database)
        self.player_database = str(player_database)
        self.clock = clock or SystemClock()
        self.lock = lock or RLock()

    def _now(self) -> datetime:
        return self.clock.now()

    def _timestamp(self, value: Any | None = None) -> str:
        if value is None:
            value = self._now()
        if isinstance(value, datetime):
            return value.strftime("%Y-%m-%d %H:%M:%S")
        return str(value)

    def _normalize_date(self, value: Any | None) -> str:
        if value is None:
            value = self._now()
        if isinstance(value, datetime):
            value = value.date()
        if isinstance(value, date):
            return value.isoformat()
        return date.fromisoformat(str(value).strip()).isoformat()

    @staticmethod
    def _payload(operator_id: str) -> str:
        return json.dumps(
            {"action": "training_reset", "operator_id": operator_id},
            ensure_ascii=True,
            sort_keys=True,
            separators=(",", ":"),
        )

    @classmethod
    def _snapshot(cls, row: dict[str, Any]) -> str:
        return json.dumps(
            {field: row.get(field) for field in cls._STATE_FIELDS},
            ensure_ascii=True,
            sort_keys=True,
            separators=(",", ":"),
            default=str,
        )

    @staticmethod
    def _weekly(value: Any) -> Any:
        if value in (None, ""):
            return {}
        try:
            decoded = json.loads(str(value))
        except (TypeError, ValueError, json.JSONDecodeError):
            return value
        return decoded

    @classmethod
    def _state_changed(cls, row: dict[str, Any], weekly: dict[str, str]) -> bool:
        return (
            int(row.get("progress") or 0) != 0
            or row.get("last_time") is not None
            or int(row.get("points") or 0) != 0
            or int(row.get("completed") or 0) != 0
            or int(row.get("max_progress") or 0) != 0
            or str(row.get("last_event") or "") != ""
            or cls._weekly(row.get("weekly_purchases")) != weekly
        )

    @staticmethod
    def _tables(uow: AttachedDatabaseUnitOfWork, schema: str = "main") -> set[str]:
        prefix = "" if schema == "main" else f"{schema}."
        return {
            str(row["name"])
            for row in uow.query_all(
                f"SELECT name FROM {prefix}sqlite_master WHERE type='table'"
            )
        }

    @staticmethod
    def _columns(uow: AttachedDatabaseUnitOfWork, table: str, schema: str = "main") -> set[str]:
        pragma = f"PRAGMA {schema}.table_info({table})" if schema != "main" else f"PRAGMA table_info({table})"
        return {str(row["name"]) for row in uow.query_all(pragma)}

    @classmethod
    def _schema_ready(cls, uow: AttachedDatabaseUnitOfWork) -> bool:
        if not cls._GAME_TABLES.issubset(cls._tables(uow)):
            return False
        if not cls._PLAYER_TABLES.issubset(cls._tables(uow, "player_data")):
            return False
        if not cls._OPERATION_COLUMNS.issubset(
            cls._columns(uow, "admin_training_reset_operations")
        ):
            return False
        if not cls._TARGET_COLUMNS.issubset(
            cls._columns(uow, "admin_training_reset_targets")
        ):
            return False
        return set(cls._STATE_FIELDS).issubset(
            cls._columns(uow, "training", "player_data")
        )

    @classmethod
    def _result(
        cls,
        uow: AttachedDatabaseUnitOfWork,
        operation_id: str,
        status: str,
    ) -> TrainingResetResult:
        row = uow.query_one(
            "SELECT status,reset_date,total,completed,changed,skipped "
            "FROM admin_training_reset_operations WHERE operation_id=?",
            (operation_id,),
        )
        if row is None:
            return TrainingResetResult(status)
        return TrainingResetResult(
            status=status,
            task_status=str(row["status"]),
            reset_date=str(row["reset_date"]),
            total=int(row["total"]),
            completed=int(row["completed"]),
            changed=int(row["changed"]),
            skipped=int(row["skipped"]),
        )

    def reset(
        self,
        operation_id: str,
        operator_id: str,
        *,
        chunk_size: int = 500,
        reset_date: Any | None = None,
        updated_at: Any | None = None,
    ) -> TrainingResetResult:
        operation_id = str(operation_id).strip()
        operator_id = str(operator_id).strip()
        chunk_size = max(1, int(chunk_size))
        if not operation_id or not operator_id:
            raise ValueError("operation and operator are required")
        reset_date = self._normalize_date(reset_date)
        updated_at = self._timestamp(updated_at)
        payload = self._payload(operator_id)
        weekly = {"_last_reset": reset_date}
        weekly_json = json.dumps(weekly, ensure_ascii=True, sort_keys=True, separators=(",", ":"))

        with self.lock, AttachedDatabaseUnitOfWork(
            self.game_database,
            attachments={"player_data": self.player_database},
            immediate=True,
        ) as uow:
            if not self._schema_ready(uow):
                return TrainingResetResult("schema_missing")

            operation = uow.query_one(
                "SELECT payload,reset_date,status FROM admin_training_reset_operations "
                "WHERE operation_id=?",
                (operation_id,),
            )
            if operation is None:
                user_ids = tuple(
                    str(row["user_id"])
                    for row in uow.query_all(
                        "SELECT DISTINCT user_id FROM user_xiuxian ORDER BY user_id"
                    )
                )
                task_status = "completed" if not user_ids else "running"
                uow.execute(
                    "INSERT INTO admin_training_reset_operations("
                    "operation_id,payload,reset_date,total,status,created_at,updated_at) "
                    "VALUES(?,?,?,?,?,?,?)",
                    (
                        operation_id,
                        payload,
                        reset_date,
                        len(user_ids),
                        task_status,
                        updated_at,
                        updated_at,
                    ),
                )
                uow.executemany(
                    "INSERT INTO admin_training_reset_targets(operation_id,user_id,updated_at) "
                    "VALUES(?,?,?)",
                    ((operation_id, user_id, updated_at) for user_id in user_ids),
                )
                if not user_ids:
                    return self._result(uow, operation_id, "applied")
            else:
                if str(operation["payload"]) != payload:
                    return self._result(uow, operation_id, "operation_conflict")
                reset_date = str(operation["reset_date"])
                weekly = {"_last_reset": reset_date}
                weekly_json = json.dumps(weekly, ensure_ascii=True, sort_keys=True, separators=(",", ":"))
                if str(operation["status"]) == "completed":
                    return self._result(uow, operation_id, "duplicate")

            pending = uow.query_all(
                "SELECT user_id FROM admin_training_reset_targets "
                "WHERE operation_id=? AND status='pending' ORDER BY user_id LIMIT ?",
                (operation_id, chunk_size),
            )
            changed = 0
            skipped = 0
            for pending_row in pending:
                user_id = str(pending_row["user_id"])
                user_exists = uow.query_one(
                    "SELECT 1 AS present FROM user_xiuxian WHERE user_id=?", (user_id,)
                )
                training = None
                if user_exists is not None:
                    training = uow.query_one(
                        "SELECT progress,last_time,points,completed,max_progress,last_event,"
                        "weekly_purchases FROM player_data.training WHERE user_id=?",
                        (user_id,),
                    )
                if training is None:
                    skipped += 1
                    uow.execute(
                        "UPDATE admin_training_reset_targets SET status='skipped',updated_at=? "
                        "WHERE operation_id=? AND user_id=? AND status='pending'",
                        (updated_at, operation_id, user_id),
                    )
                    continue

                changed += int(self._state_changed(training, weekly))
                previous_state = self._snapshot(training)
                updated = uow.execute(
                    "UPDATE player_data.training SET progress=0,last_time=NULL,points=0,"
                    "completed=0,max_progress=0,last_event='',weekly_purchases=? "
                    "WHERE user_id=?",
                    (weekly_json, user_id),
                )
                if updated.rowcount != 1:
                    raise RuntimeError("training reset target changed")
                target_updated = uow.execute(
                    "UPDATE admin_training_reset_targets SET status='applied',"
                    "previous_state=?,updated_at=? WHERE operation_id=? AND user_id=? "
                    "AND status='pending'",
                    (previous_state, updated_at, operation_id, user_id),
                )
                if target_updated.rowcount != 1:
                    raise RuntimeError("training reset progress changed")

            progress = uow.query_one(
                "SELECT COUNT(*) AS total,COALESCE(SUM(CASE WHEN status='pending' "
                "THEN 1 ELSE 0 END),0) AS pending FROM admin_training_reset_targets "
                "WHERE operation_id=?",
                (operation_id,),
            )
            completed = int(progress["total"]) - int(progress["pending"])
            task_status = "completed" if int(progress["pending"]) == 0 else "running"
            uow.execute(
                "UPDATE admin_training_reset_operations SET completed=?,changed=changed+?,"
                "skipped=skipped+?,status=?,updated_at=? WHERE operation_id=?",
                (completed, changed, skipped, task_status, updated_at, operation_id),
            )
            return self._result(uow, operation_id, "applied")


__all__ = ["TrainingResetResult", "TrainingResetSqlRepository"]
