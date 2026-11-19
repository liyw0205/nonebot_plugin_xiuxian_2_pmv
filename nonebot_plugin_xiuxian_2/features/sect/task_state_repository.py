from __future__ import annotations

import json
from functools import wraps
from pathlib import Path
from typing import Any

from ...infrastructure.database import DatabaseUnitOfWork
from ...core.errors import DomainError
from ...core.numeric import require_exact_integer


def _verified_task_result(method):
    # Convert known write refusals only after the method's transaction rolled back.
    @wraps(method)
    def call(*args, **kwargs):
        try:
            return method(*args, **kwargs)
        except DomainError as exc:
            if not exc.code.startswith('sect_task_') or not exc.code.endswith('_write_rejected'):
                raise
            return {'status': 'state_changed', 'reason': exc.code, **exc.details}
    return call


def _task_object(value, subject):
    try:
        data = json.loads(value)
        if not isinstance(data, dict):
            raise ValueError('task must be an object')
        return data
    except (ValueError, TypeError) as exc:
        raise DomainError('sect_task_data_invalid', 'invalid task data', {'subject': str(subject)}) from exc


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
            raise DomainError("schema_missing", "sect task schema is not ready")

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
            raise DomainError("schema_missing", "sect task claim schema is not ready")

    def assert_schema_ready(self) -> None:
        with DatabaseUnitOfWork(self.database, read_only=True) as uow:
            self._assert_schema_ready(uow)

    @staticmethod
    def _reject(mode, step, subject):
        reason = f'sect_task_{mode}_{step}_write_rejected'
        raise DomainError(reason, reason, {'reason': reason, 'subject': str(subject)})

    def _verify_claim(self, uow, mode, expected, now):
        operation_id, user_id, sect_id, period, task_key, task_json = expected
        saved = uow.query_one('SELECT sect_id,task_key,task_data,status,progress,target,accepted_at,updated_at,completed_at '
                              'FROM sect_task_state WHERE user_id=? AND period=?', (user_id, period))
        if saved != dict(sect_id=sect_id, task_key=task_key, task_data=task_json, status='accepted',
                         progress=0, target=1, accepted_at=now, updated_at=now, completed_at=None):
            self._reject(mode, 'state', user_id)
        receipt = uow.query_one('SELECT operation_id,user_id,sect_id,period,task_key,task_data '
                                'FROM sect_task_claim_operations WHERE operation_id=?', (operation_id,))
        if receipt != dict(zip(('operation_id', 'user_id', 'sect_id', 'period', 'task_key', 'task_data'), expected)):
            self._reject(mode, 'receipt', operation_id)

    def claim_receipt(self, operation_id, user_id):
        with DatabaseUnitOfWork(self.database, read_only=True) as uow:
            self._assert_claim_schema_ready(uow)
            row = uow.query_one('SELECT user_id,sect_id,period,task_key,task_data FROM sect_task_claim_operations '
                                'WHERE operation_id=?', (str(operation_id),))
            if row is None:
                return None
            if str(row['user_id']) != str(user_id):
                raise DomainError('operation_conflict', 'task receipt belongs to another player')
            return {'status': 'duplicate', **row, 'task_data': _task_object(row['task_data'], operation_id)}

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

    @_verified_task_result
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
                    "task_data": _task_object(previous["task_data"], operation_id),
                }

            user = uow.query_one(
                "SELECT sect_id,sect_task FROM user_xiuxian WHERE user_id=?", (user_id,)
            )
            if user is None:
                return {"status": "user_missing", "user_id": user_id, "sect_id": sect_id}
            if user["sect_id"] is None or int(user["sect_id"]) != sect_id:
                return {"status": "sect_changed", "reason": "user_missing" if user is None else "sect_changed", "user_id": user_id, "sect_id": sect_id}
            if require_exact_integer(user["sect_task"] or 0, field='sect_task', subject=user_id) >= daily_limit:
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
            changed = uow.execute(
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
            if changed.rowcount != 1:
                self._reject('claim', 'state', user_id)
            recorded = uow.execute(
                "INSERT INTO sect_task_claim_operations "
                "(operation_id,user_id,sect_id,period,task_key,task_data) VALUES(?,?,?,?,?,?)",
                (operation_id, user_id, sect_id, period, task_key, task_json),
            )
            if recorded.rowcount != 1:
                self._reject('claim', 'receipt', operation_id)
            self._verify_claim(uow, 'claim', (operation_id, user_id, sect_id, period, task_key, task_json), now)
        return {
            "status": "claimed", "user_id": user_id, "sect_id": sect_id,
            "period": period, "task_key": task_key, "task_data": task_data,
        }

    @_verified_task_result
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
                    "task_data": _task_object(previous["task_data"], operation_id),
                }

            user = uow.query_one(
                "SELECT sect_id,sect_task FROM user_xiuxian WHERE user_id=?", (user_id,)
            )
            if user is None or user["sect_id"] is None or int(user["sect_id"]) != sect_id:
                return {"status": "sect_changed", "reason": "user_missing" if user is None else "sect_changed", "user_id": user_id, "sect_id": sect_id}
            if require_exact_integer(user["sect_task"] or 0, field='sect_task', subject=user_id) >= daily_limit:
                return {"status": "daily_limit", "user_id": user_id, "sect_id": sect_id}
            if uow.query_one('SELECT 1 FROM sects WHERE sect_id=?', (sect_id,)) is None:
                return {'status': 'sect_missing', 'user_id': user_id, 'sect_id': sect_id}
            current = uow.query_one(
                "SELECT sect_id,task_key,task_data,status FROM sect_task_state WHERE user_id=? AND period=?",
                (user_id, period),
            )
            if current is None or str(current["status"]) != "accepted":
                return {"status": "task_missing", "reason": "task_missing" if current is None else "task_resolved", "user_id": user_id, "sect_id": sect_id}
            if int(current['sect_id']) != sect_id:
                return {'status': 'task_sect_changed', 'reason': 'task_sect_changed', 'subject': user_id}
            if (
                str(current["task_key"]) != expected_task_key
                or _task_object(current["task_data"], user_id) != expected_task_data
            ):
                return {**{"status": "state_changed", "user_id": user_id, "sect_id": sect_id}, "reason": "task_key_changed" if str(current["task_key"]) != str(expected_task_key) else "task_contents_changed", "subject": ""}

            task_json = _dump_task_data(task_data)
            changed = uow.execute(
                """UPDATE sect_task_state SET task_key=?,task_data=?,progress=0,target=1,
                       accepted_at=?,updated_at=?,completed_at=NULL
                   WHERE user_id=? AND period=? AND status='accepted'""",
                (task_key, task_json, now, now, user_id, period),
            )
            if changed.rowcount != 1:
                self._reject('refresh', 'state', user_id)
            recorded = uow.execute(
                "INSERT INTO sect_task_claim_operations "
                "(operation_id,user_id,sect_id,period,task_key,task_data) VALUES(?,?,?,?,?,?)",
                (operation_id, user_id, sect_id, period, task_key, task_json),
            )
            if recorded.rowcount != 1:
                self._reject('refresh', 'receipt', operation_id)
            self._verify_claim(uow, 'refresh', (operation_id, user_id, sect_id, period, task_key, task_json), now)
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
