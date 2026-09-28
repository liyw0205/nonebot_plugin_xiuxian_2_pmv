from __future__ import annotations

import json
from typing import Any, Mapping

from ...infrastructure.database import DatabaseUnitOfWork

STATE_FIELDS = (
    "active", "status", "event_id", "event_type", "name", "period", "manual",
    "bosses", "participants", "claimed", "started_at", "ends_at", "last_result",
)
JSON_FIELDS = {"bosses", "participants", "claimed"}
INTEGER_FIELDS = {"active", "manual"}


def decode_event_state_value(field: str, value: Any) -> Any:
    if field in JSON_FIELDS:
        if isinstance(value, (dict, list)):
            return value
        try:
            return json.loads(value or "{}")
        except (TypeError, ValueError):
            return {}
    if field in INTEGER_FIELDS:
        try:
            return int(value or 0)
        except (TypeError, ValueError):
            return 0
    return str(value or "")


def encode_event_state_value(field: str, value: Any) -> Any:
    if field in JSON_FIELDS:
        return json.dumps(value or {}, ensure_ascii=False, sort_keys=True)
    if field in INTEGER_FIELDS:
        return int(value or 0)
    return str(value or "")


def event_state_schema_ready(uow: DatabaseUnitOfWork) -> bool:
    if uow.query_one(
        "SELECT 1 FROM sqlite_master WHERE type='table' AND name='world_event_state'"
    ) is None:
        return False
    columns = {
        row["name"] for row in uow.query_all("PRAGMA table_info(world_event_state)")
    }
    return {"user_id", *STATE_FIELDS}.issubset(columns)


def read_event_state(
    uow: DatabaseUnitOfWork, event_key: str
) -> dict[str, Any] | None:
    fields = ",".join(f'"{field}"' for field in STATE_FIELDS)
    row = uow.query_one(
        f"SELECT {fields} FROM world_event_state WHERE user_id=?", (event_key,)
    )
    if row is None:
        return None
    return {
        field: decode_event_state_value(field, row[field])
        for field in STATE_FIELDS
    }


def write_event_state(
    uow: DatabaseUnitOfWork, event_key: str, state: Mapping[str, Any]
) -> None:
    assignments = ",".join(f'"{field}"=?' for field in STATE_FIELDS)
    values = [encode_event_state_value(field, state.get(field)) for field in STATE_FIELDS]
    changed = uow.execute(
        f"UPDATE world_event_state SET {assignments} WHERE user_id=?",
        (*values, event_key),
    )
    if changed.rowcount:
        return
    fields = ",".join(["user_id", *[f'"{field}"' for field in STATE_FIELDS]])
    marks = ",".join("?" for _ in range(len(STATE_FIELDS) + 1))
    uow.execute(
        f"INSERT INTO world_event_state ({fields}) VALUES ({marks})",
        (event_key, *values),
    )


def verify_event_state(
    uow: DatabaseUnitOfWork, event_key: str, expected: Mapping[str, Any]
) -> None:
    if read_event_state(uow, event_key) != dict(expected):
        raise RuntimeError("world event state verification failed")


__all__ = [
    "STATE_FIELDS",
    "JSON_FIELDS",
    "INTEGER_FIELDS",
    "decode_event_state_value",
    "encode_event_state_value",
    "event_state_schema_ready",
    "read_event_state",
    "write_event_state",
    "verify_event_state",
]
