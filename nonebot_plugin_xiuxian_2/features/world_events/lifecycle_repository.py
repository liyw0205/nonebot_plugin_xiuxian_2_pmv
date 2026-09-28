from __future__ import annotations

import json
from dataclasses import asdict
from datetime import datetime
from pathlib import Path
from typing import Any, Mapping, Protocol

from ...infrastructure.database import DatabaseUnitOfWork
from .domain import DemonEventLifecycleResult, SpiritVeinLifecycleResult
from .event_state import (
    INTEGER_FIELDS,
    JSON_FIELDS,
    STATE_FIELDS,
    decode_event_state_value,
    encode_event_state_value,
    event_state_schema_ready,
    read_event_state,
    verify_event_state,
    write_event_state,
)


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
        return decode_event_state_value(field, value)

    @staticmethod
    def _encode(field: str, value: Any) -> Any:
        return encode_event_state_value(field, value)

    @staticmethod
    def _schema_ready(uow: DatabaseUnitOfWork) -> bool:
        tables = {
            row["name"]
            for row in uow.query_all("SELECT name FROM sqlite_master WHERE type='table'")
        }
        return (
            "demon_event_lifecycle_operations" in tables
            and event_state_schema_ready(uow)
        )

    @classmethod
    def _read_state(cls, uow: DatabaseUnitOfWork, event_key: str) -> dict[str, Any] | None:
        return read_event_state(uow, event_key)

    @classmethod
    def _write_state(cls, uow: DatabaseUnitOfWork, event_key: str, state: Mapping[str, Any]) -> None:
        write_event_state(uow, event_key, state)

    @classmethod
    def _verify_state(
        cls, uow: DatabaseUnitOfWork, event_key: str, expected: Mapping[str, Any]
    ) -> None:
        try:
            verify_event_state(uow, event_key, expected)
        except RuntimeError as exc:
            raise RuntimeError("demon lifecycle state verification failed") from exc

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


class SpiritVeinLifecycleRepository(Protocol):
    def replay(self, operation_id: str) -> SpiritVeinLifecycleResult | None: ...

    def transition(
        self, operation_id: str, event_key: str, action: str,
        expected_state: Mapping[str, Any] | None, target_state: Mapping[str, Any],
    ) -> SpiritVeinLifecycleResult: ...


class SpiritVeinLifecycleSqlRepository:
    """Owns the player-database transaction for spirit vein state transitions."""

    _ACTIONS = {
        "auto_start", "auto_skip", "auto_miss", "manual_start",
        "manual_start_skip", "manual_finish", "manual_finish_skip", "expire",
    }
    _NOOP_ACTIONS = {
        "auto_skip", "auto_miss", "manual_start_skip", "manual_finish_skip",
    }
    _RESULT_STATUS = {
        "auto_start": "applied",
        "auto_skip": "already_active",
        "auto_miss": "not_triggered",
        "manual_start": "applied",
        "manual_start_skip": "already_active",
        "manual_finish": "applied",
        "manual_finish_skip": "already_finished",
        "expire": "applied",
    }

    def __init__(self, player_database: str | Path) -> None:
        self.player_database = str(player_database)

    @staticmethod
    def _schema_ready(uow: DatabaseUnitOfWork) -> bool:
        return (
            uow.query_one(
                "SELECT 1 FROM sqlite_master WHERE type='table' "
                "AND name='spirit_vein_lifecycle_operations'"
            ) is not None
            and event_state_schema_ready(uow)
        )

    @staticmethod
    def _normalize(state: Mapping[str, Any] | None) -> dict[str, Any] | None:
        if state is None:
            return None
        return {
            field: decode_event_state_value(field, state.get(field))
            for field in STATE_FIELDS
        }

    @staticmethod
    def _parse_time(value: Any):
        try:
            return datetime.fromisoformat(str(value)) if value else None
        except ValueError:
            return None

    @classmethod
    def _valid_transition(
        cls, action: str, current: Mapping[str, Any] | None, target: Mapping[str, Any]
    ) -> bool:
        current_state = current or target
        if action in cls._NOOP_ACTIONS:
            if dict(target) != dict(current_state):
                return False
            if action in {"auto_skip", "manual_start_skip"}:
                return current is not None and current.get("status") == "active"
            if action == "auto_miss":
                return current is None or current.get("status") != "active"
            return current is None or current.get("status") != "active"

        if action in {"auto_start", "manual_start"}:
            started_at = cls._parse_time(target.get("started_at"))
            ends_at = cls._parse_time(target.get("ends_at"))
            return (
                (current is None or current.get("status") != "active")
                and target.get("status") == "active"
                and int(target.get("active") or 0) == 1
                and target.get("event_type") == "spirit_vein"
                and bool(target.get("event_id"))
                and started_at is not None
                and ends_at is not None
                and started_at < ends_at
            )

        return (
            current is not None
            and current.get("status") == "active"
            and target.get("event_id") == current.get("event_id")
            and target.get("started_at") == current.get("started_at")
            and target.get("ends_at") == current.get("ends_at")
            and target.get("status") == "finished"
            and int(target.get("active") or 0) == 0
        )

    def replay(self, operation_id: str) -> SpiritVeinLifecycleResult | None:
        with DatabaseUnitOfWork(self.player_database, read_only=True) as uow:
            if not self._schema_ready(uow):
                return None
            row = uow.query_one(
                "SELECT result_json FROM spirit_vein_lifecycle_operations "
                "WHERE operation_id=?",
                (str(operation_id),),
            )
        return (
            None
            if row is None
            else SpiritVeinLifecycleResult(**json.loads(str(row["result_json"])))
        )

    def transition(
        self,
        operation_id: str,
        event_key: str,
        action: str,
        expected_state: Mapping[str, Any] | None,
        target_state: Mapping[str, Any],
    ) -> SpiritVeinLifecycleResult:
        operation_id = str(operation_id).strip()
        event_key = str(event_key).strip()
        action = str(action).strip()
        if not operation_id or not event_key or action not in self._ACTIONS:
            raise ValueError("invalid spirit vein lifecycle operation")
        expected = self._normalize(expected_state)
        target = self._normalize(target_state)
        if target is None:
            raise ValueError("target state is required")
        payload = json.dumps(
            {
                "event_key": event_key,
                "action": action,
                "expected_state": expected,
                "target_state": target,
            },
            ensure_ascii=True,
            sort_keys=True,
            separators=(",", ":"),
        )

        with DatabaseUnitOfWork(self.player_database, immediate=True) as uow:
            if not self._schema_ready(uow):
                return SpiritVeinLifecycleResult("schema_missing", action)
            previous = uow.query_one(
                "SELECT payload,result_json FROM spirit_vein_lifecycle_operations "
                "WHERE operation_id=?",
                (operation_id,),
            )
            if previous is not None:
                if str(previous["payload"]) != payload:
                    return SpiritVeinLifecycleResult("operation_conflict", action)
                return SpiritVeinLifecycleResult(
                    **json.loads(str(previous["result_json"]))
                )

            current = read_event_state(uow, event_key)
            first_idle_state = (
                current is None
                and expected is not None
                and expected.get("status") == "idle"
                and not expected.get("event_id")
            )
            if current != expected and not first_idle_state:
                return SpiritVeinLifecycleResult("state_changed", action, current)
            effective_current = current if current is not None else expected
            if not self._valid_transition(action, effective_current, target):
                return SpiritVeinLifecycleResult(
                    "invalid_transition", action, effective_current
                )

            if action not in self._NOOP_ACTIONS:
                write_event_state(uow, event_key, target)
                try:
                    verify_event_state(uow, event_key, target)
                except RuntimeError as exc:
                    raise RuntimeError("spirit vein lifecycle state verification failed") from exc
            result = SpiritVeinLifecycleResult(
                self._RESULT_STATUS[action], action, target
            )
            uow.execute(
                "INSERT INTO spirit_vein_lifecycle_operations "
                "(operation_id,payload,result_json,created_at) "
                "VALUES(?,?,?,CURRENT_TIMESTAMP)",
                (
                    operation_id,
                    payload,
                    json.dumps(
                        asdict(result),
                        ensure_ascii=True,
                        sort_keys=True,
                        separators=(",", ":"),
                    ),
                ),
            )
            return result


__all__ = [
    "DemonEventLifecycleRepository",
    "DemonEventLifecycleSqlRepository",
    "SpiritVeinLifecycleRepository",
    "SpiritVeinLifecycleSqlRepository",
    "STATE_FIELDS",
    "JSON_FIELDS",
    "INTEGER_FIELDS",
]
