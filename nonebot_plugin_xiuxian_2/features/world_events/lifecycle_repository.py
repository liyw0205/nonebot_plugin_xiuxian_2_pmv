from __future__ import annotations

import json
from dataclasses import asdict
from pathlib import Path
from typing import Any, Mapping, Protocol

from ...infrastructure.database import DatabaseUnitOfWork
from .domain import DemonEventLifecycleResult

STATE_FIELDS = (
    "active", "status", "event_id", "event_type", "name", "period", "manual",
    "bosses", "participants", "claimed", "started_at", "ends_at", "last_result",
)
JSON_FIELDS = {"bosses", "participants", "claimed"}
INTEGER_FIELDS = {"active", "manual"}


class DemonEventLifecycleRepository(Protocol):
    def replay(self, operation_id: str) -> DemonEventLifecycleResult | None: ...

    def transition(
        self, operation_id: str, event_key: str, action: str,
        expected_state: Mapping[str, Any] | None, target_state: Mapping[str, Any],
    ) -> DemonEventLifecycleResult: ...


class DemonEventLifecycleSqlRepository:
    """Owns the player-database transaction for demon event state transitions."""

    def __init__(self, player_database: str | Path) -> None:
        self.player_database = str(player_database)

    @staticmethod
    def _decode(field: str, value: Any) -> Any:
        if field in JSON_FIELDS:
            if isinstance(value, (dict, list)):
                return value
            try:
                return json.loads(value or "{}")
            except (TypeError, ValueError, json.JSONDecodeError):
                return {}
        if field in INTEGER_FIELDS:
            try:
                return int(value or 0)
            except (TypeError, ValueError):
                return 0
        return str(value or "")

    @staticmethod
    def _encode(field: str, value: Any) -> Any:
        if field in JSON_FIELDS:
            return json.dumps(value or {}, ensure_ascii=False, sort_keys=True)
        if field in INTEGER_FIELDS:
            return int(value or 0)
        return str(value or "")

    @staticmethod
    def _schema_ready(uow: DatabaseUnitOfWork) -> bool:
        tables = {
            row["name"]
            for row in uow.query_all("SELECT name FROM sqlite_master WHERE type='table'")
        }
        if not {"world_event_state", "demon_event_lifecycle_operations"}.issubset(tables):
            return False
        columns = {
            row["name"] for row in uow.query_all("PRAGMA table_info(world_event_state)")
        }
        return {"user_id", *STATE_FIELDS}.issubset(columns)

    @classmethod
    def _read_state(cls, uow: DatabaseUnitOfWork, event_key: str) -> dict[str, Any] | None:
        fields = ",".join(f'"{field}"' for field in STATE_FIELDS)
        row = uow.query_one(
            f"SELECT {fields} FROM world_event_state WHERE user_id=?", (event_key,)
        )
        if row is None:
            return None
        return {
            field: cls._decode(field, row[field])
            for field in STATE_FIELDS
        }

    @classmethod
    def _write_state(cls, uow: DatabaseUnitOfWork, event_key: str, state: Mapping[str, Any]) -> None:
        assignments = ",".join(f'"{field}"=?' for field in STATE_FIELDS)
        values = [cls._encode(field, state.get(field)) for field in STATE_FIELDS]
        changed = uow.execute(
            f"UPDATE world_event_state SET {assignments} WHERE user_id=?",
            (*values, event_key),
        )
        if changed.rowcount == 0:
            fields = ",".join(["user_id", *[f'"{field}"' for field in STATE_FIELDS]])
            marks = ",".join("?" for _ in range(len(STATE_FIELDS) + 1))
            uow.execute(
                f"INSERT INTO world_event_state ({fields}) VALUES ({marks})",
                (event_key, *values),
            )

    @classmethod
    def _verify_state(
        cls, uow: DatabaseUnitOfWork, event_key: str, expected: Mapping[str, Any]
    ) -> None:
        if cls._read_state(uow, event_key) != dict(expected):
            raise RuntimeError("demon lifecycle state verification failed")

    def replay(self, operation_id: str) -> DemonEventLifecycleResult | None:
        operation_id = str(operation_id)
        with DatabaseUnitOfWork(self.player_database, read_only=True) as uow:
            if not self._schema_ready(uow):
                return None
            row = uow.query_one(
                "SELECT result_json FROM demon_event_lifecycle_operations WHERE operation_id=?",
                (operation_id,),
            )
        return (
            None
            if row is None
            else DemonEventLifecycleResult(**json.loads(str(row["result_json"])))
        )

    def transition(
        self,
        operation_id: str,
        event_key: str,
        action: str,
        expected_state: Mapping[str, Any] | None,
        target_state: Mapping[str, Any],
    ) -> DemonEventLifecycleResult:
        operation_id, action = str(operation_id).strip(), str(action).strip()
        if not operation_id or action not in {
            "auto_start", "manual_start", "auto_finish", "manual_finish"
        }:
            raise ValueError("invalid lifecycle operation")
        expected = (
            None
            if expected_state is None
            else {field: self._decode(field, expected_state.get(field)) for field in STATE_FIELDS}
        )
        target = {field: self._decode(field, target_state.get(field)) for field in STATE_FIELDS}
        event_key = str(event_key)
        payload = json.dumps(
            {
                "event_key": event_key,
                "action": action,
                "expected_state": expected,
                "target_state": target,
            },
            ensure_ascii=False,
            sort_keys=True,
            separators=(",", ":"),
        )

        with DatabaseUnitOfWork(self.player_database, immediate=True) as uow:
            if not self._schema_ready(uow):
                return DemonEventLifecycleResult("schema_missing", action)
            previous = uow.query_one(
                "SELECT payload,result_json FROM demon_event_lifecycle_operations WHERE operation_id=?",
                (operation_id,),
            )
            if previous is not None:
                if str(previous["payload"]) != payload:
                    return DemonEventLifecycleResult("operation_conflict", action)
                return DemonEventLifecycleResult(
                    **json.loads(str(previous["result_json"]))
                )

            current = self._read_state(uow, event_key)
            first_start = (
                current is None
                and action.endswith("start")
                and expected is not None
                and expected.get("status") == "idle"
                and not expected.get("event_id")
            )
            if current != expected and not first_start:
                return DemonEventLifecycleResult("state_changed", action, current)
            if action.endswith("start"):
                valid = (
                    target.get("status") == "active"
                    and int(target.get("active") or 0) == 1
                    and bool(target.get("event_id"))
                )
                valid = valid and not (current and current.get("status") == "active")
            else:
                valid = current is not None and current.get("status") == "active"
                valid = valid and target.get("event_id") == current.get("event_id")
                valid = valid and target.get("status") == "finished"
                valid = valid and int(target.get("active") or 0) == 0
            if not valid:
                return DemonEventLifecycleResult("invalid_transition", action, current)

            self._write_state(uow, event_key, target)
            self._verify_state(uow, event_key, target)
            result = DemonEventLifecycleResult("applied", action, target)
            uow.execute(
                "INSERT INTO demon_event_lifecycle_operations "
                "(operation_id,payload,result_json,created_at) VALUES(?,?,?,CURRENT_TIMESTAMP)",
                (
                    operation_id,
                    payload,
                    json.dumps(asdict(result), ensure_ascii=False, sort_keys=True),
                ),
            )
            return result


__all__ = [
    "DemonEventLifecycleRepository",
    "DemonEventLifecycleSqlRepository",
    "STATE_FIELDS",
    "JSON_FIELDS",
    "INTEGER_FIELDS",
]
