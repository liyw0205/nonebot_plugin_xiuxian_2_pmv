from __future__ import annotations

import json
from pathlib import Path
from typing import Any

from ...infrastructure.database import DatabaseUnitOfWork


def _dump_task_data(task_data: dict[str, Any]) -> str:
    try:
        return json.dumps(task_data, ensure_ascii=False)
    except (TypeError, ValueError):
        return json.dumps(str(task_data), ensure_ascii=False)


def _load_task_data(value: Any) -> dict[str, Any]:
    try:
        task_data = json.loads(value) if value not in (None, "") else {}
    except (TypeError, ValueError):
        return {}
    return task_data if isinstance(task_data, dict) else {}


class SectTaskStateSqlRepository:
    """Owns persisted daily sect task state without request-time DDL."""

    _REQUIRED_COLUMNS = {
        "user_id",
        "sect_id",
        "task_key",
        "task_data",
        "period",
        "status",
        "progress",
        "target",
        "accepted_at",
        "updated_at",
        "completed_at",
    }
    _CLAIM_OPERATION_COLUMNS = {
        "operation_id", "user_id", "sect_id", "period", "task_key", "task_data", "created_at",
    }

    def __init__(self, database: str | Path) -> None:
        self.database = str(database)

    @classmethod
    def _assert_schema_ready(cls, uow: DatabaseUnitOfWork) -> None:
        table = uow.query_one(
            "SELECT 1 AS present FROM sqlite_master WHERE type='table' AND name=?",
            ("sect_task_state",),
        )
        columns = {
            str(row["name"]) for row in uow.query_all("PRAGMA table_info(sect_task_state)")
        }
        if table is None or not cls._REQUIRED_COLUMNS.issubset(columns):
            raise RuntimeError("sect_task_state schema is not ready; run migrations first")

    @classmethod
    def _assert_claim_schema_ready(cls, uow: DatabaseUnitOfWork) -> None:
        cls._assert_schema_ready(uow)
        table = uow.query_one(
            "SELECT 1 AS present FROM sqlite_master WHERE type='table' AND name=?",
            ("sect_task_claim_operations",),
        )
        columns = {
            str(row["name"])
            for row in uow.query_all("PRAGMA table_info(sect_task_claim_operations)")
        }
        if table is None or not cls._CLAIM_OPERATION_COLUMNS.issubset(columns):
            raise RuntimeError("sect_task_claim_operations schema is not ready; run migrations first")

    def assert_schema_ready(self) -> None:
        with DatabaseUnitOfWork(self.database, read_only=True) as uow:
            self._assert_schema_ready(uow)

    @staticmethod
    def _row_to_task(row: dict[str, Any] | None) -> dict[str, Any] | None:
        if row is None:
            return None
        task_data = _load_task_data(row["task_data"])
        return {
            "任务名称": row["task_key"],
            "任务内容": task_data,
            "sect_id": row["sect_id"],
            "period": row["period"],
            "status": row["status"],
            "progress": row["progress"],
            "target": row["target"],
            "accepted_at": row["accepted_at"],
            "updated_at": row["updated_at"],
            "completed_at": row["completed_at"],
        }

    def get_active_task(self, user_id: str | int, period: str) -> dict[str, Any] | None:
        with DatabaseUnitOfWork(self.database, read_only=True) as uow:
            self._assert_schema_ready(uow)
            row = uow.query_one(
                "SELECT * FROM sect_task_state "
                "WHERE user_id=? AND period=? AND status='accepted' LIMIT 1",
                (str(user_id), str(period)),
            )
        return self._row_to_task(row)

    def accept_task(
        self,
        user_id: str | int,
        sect_id: str | int,
        task_key: str,
        task_data: dict[str, Any],
        period: str,
        now: str,
    ) -> dict[str, Any]:
        user_id, sect_id, task_key, period = str(user_id), int(sect_id), str(task_key), str(period)
        task_data = dict(task_data)
        with DatabaseUnitOfWork(self.database, immediate=True) as uow:
            self._assert_schema_ready(uow)
            uow.execute(
                """INSERT INTO sect_task_state (
                       user_id, sect_id, task_key, task_data, period, status,
                       progress, target, accepted_at, updated_at, completed_at
                   ) VALUES (?, ?, ?, ?, ?, 'accepted', 0, 1, ?, ?, NULL)
                   ON CONFLICT(user_id, period) DO UPDATE SET
                       sect_id=excluded.sect_id,
                       task_key=excluded.task_key,
                       task_data=excluded.task_data,
                       status='accepted',
                       progress=0,
                       target=1,
                       accepted_at=excluded.accepted_at,
                       updated_at=excluded.updated_at,
                       completed_at=NULL""",
                (user_id, sect_id, task_key, _dump_task_data(task_data), period, now, now),
            )
        return {
            "任务名称": task_key,
            "任务内容": task_data,
            "sect_id": sect_id,
            "period": period,
            "status": "accepted",
            "progress": 0,
            "target": 1,
            "accepted_at": now,
            "updated_at": now,
            "completed_at": None,
        }

    def claim_task(
        self,
        operation_id: str,
        user_id: str | int,
        sect_id: str | int,
        task_key: str,
        task_data: dict[str, Any],
        period: str,
        daily_limit: int,
        now: str,
        *,
        replace_existing: bool = False,
    ) -> dict[str, Any]:
        operation_id, user_id = str(operation_id).strip(), str(user_id)
        sect_id, task_key, period = int(sect_id), str(task_key).strip(), str(period).strip()
        task_data, daily_limit, now = dict(task_data), int(daily_limit), str(now)
        if not operation_id or not task_key or not period:
            raise ValueError("operation_id, task_key and period must not be empty")

        with DatabaseUnitOfWork(self.database, immediate=True) as uow:
            self._assert_claim_schema_ready(uow)
            previous = uow.query_one(
                "SELECT user_id,sect_id,period,task_key,task_data "
                "FROM sect_task_claim_operations WHERE operation_id=?",
                (operation_id,),
            )
            if previous is not None:
                if (str(previous["user_id"]), int(previous["sect_id"]), str(previous["period"])) != (
                    user_id, sect_id, period
                ):
                    return {"status": "operation_conflict", "user_id": user_id, "sect_id": sect_id}
                return {
                    "status": "duplicate", "user_id": user_id, "sect_id": sect_id,
                    "period": period, "task_key": str(previous["task_key"]),
                    "task_data": _load_task_data(previous["task_data"]),
                }

            user = uow.query_one(
                "SELECT sect_id,sect_task FROM user_xiuxian WHERE user_id=?", (user_id,)
            )
            if user is None:
                return {"status": "user_missing", "user_id": user_id, "sect_id": sect_id}
            if user["sect_id"] is None or int(user["sect_id"]) != sect_id:
                return {"status": "sect_changed", "user_id": user_id, "sect_id": sect_id}
            if int(user["sect_task"] or 0) >= daily_limit:
                return {"status": "daily_limit", "user_id": user_id, "sect_id": sect_id}
            if uow.query_one("SELECT 1 AS present FROM sects WHERE sect_id=?", (sect_id,)) is None:
                return {"status": "sect_missing", "user_id": user_id, "sect_id": sect_id}
            current = uow.query_one(
                "SELECT status FROM sect_task_state WHERE user_id=? AND period=?",
                (user_id, period),
            )
            if current is not None and str(current["status"]) == "accepted" and not replace_existing:
                return {"status": "task_exists", "user_id": user_id, "sect_id": sect_id}

            task_json = _dump_task_data(task_data)
            uow.execute(
                """INSERT INTO sect_task_state (
                       user_id,sect_id,task_key,task_data,period,status,progress,target,
                       accepted_at,updated_at,completed_at
                   ) VALUES (?,?,?,?,?,'accepted',0,1,?,?,NULL)
                   ON CONFLICT(user_id,period) DO UPDATE SET
                       sect_id=excluded.sect_id,task_key=excluded.task_key,
                       task_data=excluded.task_data,status='accepted',progress=0,target=1,
                       accepted_at=excluded.accepted_at,updated_at=excluded.updated_at,
                       completed_at=NULL""",
                (user_id, sect_id, task_key, task_json, period, now, now),
            )
            uow.execute(
                "INSERT INTO sect_task_claim_operations "
                "(operation_id,user_id,sect_id,period,task_key,task_data) VALUES(?,?,?,?,?,?)",
                (operation_id, user_id, sect_id, period, task_key, task_json),
            )
        return {
            "status": "claimed", "user_id": user_id, "sect_id": sect_id,
            "period": period, "task_key": task_key, "task_data": task_data,
        }

    def refresh_task(
        self,
        operation_id: str,
        user_id: str | int,
        sect_id: str | int,
        period: str,
        expected_task_key: str,
        expected_task_data: dict[str, Any],
        task_key: str,
        task_data: dict[str, Any],
        daily_limit: int,
        now: str,
    ) -> dict[str, Any]:
        operation_id, user_id = str(operation_id).strip(), str(user_id)
        sect_id, period = int(sect_id), str(period).strip()
        expected_task_key, task_key = str(expected_task_key), str(task_key).strip()
        expected_task_data, task_data = dict(expected_task_data), dict(task_data)
        daily_limit, now = int(daily_limit), str(now)
        if not operation_id or not period or not task_key:
            raise ValueError("operation_id, period and task_key must not be empty")

        with DatabaseUnitOfWork(self.database, immediate=True) as uow:
            self._assert_claim_schema_ready(uow)
            previous = uow.query_one(
                "SELECT user_id,sect_id,period,task_key,task_data "
                "FROM sect_task_claim_operations WHERE operation_id=?",
                (operation_id,),
            )
            if previous is not None:
                if (str(previous["user_id"]), int(previous["sect_id"]), str(previous["period"])) != (
                    user_id, sect_id, period
                ):
                    return {"status": "operation_conflict", "user_id": user_id, "sect_id": sect_id}
                return {
                    "status": "duplicate", "user_id": user_id, "sect_id": sect_id,
                    "period": period, "task_key": str(previous["task_key"]),
                    "task_data": _load_task_data(previous["task_data"]),
                }

            user = uow.query_one(
                "SELECT sect_id,sect_task FROM user_xiuxian WHERE user_id=?", (user_id,)
            )
            if user is None or user["sect_id"] is None or int(user["sect_id"]) != sect_id:
                return {"status": "sect_changed", "user_id": user_id, "sect_id": sect_id}
            if int(user["sect_task"] or 0) >= daily_limit:
                return {"status": "daily_limit", "user_id": user_id, "sect_id": sect_id}
            current = uow.query_one(
                "SELECT task_key,task_data,status FROM sect_task_state WHERE user_id=? AND period=?",
                (user_id, period),
            )
            if current is None or str(current["status"]) != "accepted":
                return {"status": "task_missing", "user_id": user_id, "sect_id": sect_id}
            if (
                str(current["task_key"]) != expected_task_key
                or _load_task_data(current["task_data"]) != expected_task_data
            ):
                return {"status": "state_changed", "user_id": user_id, "sect_id": sect_id}

            task_json = _dump_task_data(task_data)
            uow.execute(
                """UPDATE sect_task_state SET task_key=?,task_data=?,progress=0,target=1,
                       accepted_at=?,updated_at=?,completed_at=NULL
                   WHERE user_id=? AND period=? AND status='accepted'""",
                (task_key, task_json, now, now, user_id, period),
            )
            uow.execute(
                "INSERT INTO sect_task_claim_operations "
                "(operation_id,user_id,sect_id,period,task_key,task_data) VALUES(?,?,?,?,?,?)",
                (operation_id, user_id, sect_id, period, task_key, task_json),
            )
        return {
            "status": "claimed", "user_id": user_id, "sect_id": sect_id,
            "period": period, "task_key": task_key, "task_data": task_data,
        }

    def complete_task(self, user_id: str | int, period: str, now: str) -> None:
        with DatabaseUnitOfWork(self.database, immediate=True) as uow:
            self._assert_schema_ready(uow)
            uow.execute(
                """UPDATE sect_task_state
                   SET status='completed', progress=target, updated_at=?, completed_at=?
                   WHERE user_id=? AND period=? AND status='accepted'""",
                (now, now, str(user_id), str(period)),
            )

    def clear_task(self, user_id: str | int, period: str) -> None:
        with DatabaseUnitOfWork(self.database, immediate=True) as uow:
            self._assert_schema_ready(uow)
            uow.execute(
                "DELETE FROM sect_task_state WHERE user_id=? AND period=?",
                (str(user_id), str(period)),
            )


__all__ = ["SectTaskStateSqlRepository"]
