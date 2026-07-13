from __future__ import annotations

import json
from dataclasses import asdict, dataclass
from pathlib import Path

from ..xiuxian_utils import db_backend
from .demon_wave_refresh_service import INTEGER_FIELDS, STATE_FIELDS, _decode, _encode


@dataclass(frozen=True)
class DemonEventLifecycleResult:
    status: str
    action: str = ""
    state: dict | None = None


class DemonEventLifecycleService:
    def __init__(self, player_db: str | Path):
        self.player_db = Path(player_db)

    @staticmethod
    def _ensure_schema(conn) -> None:
        conn.execute("CREATE TABLE IF NOT EXISTS world_event_state (user_id TEXT PRIMARY KEY)")
        columns = set(conn.column_names("world_event_state"))
        for field in STATE_FIELDS:
            if field not in columns:
                data_type = "INTEGER" if field in INTEGER_FIELDS else "TEXT"
                conn.execute(f'ALTER TABLE world_event_state ADD COLUMN "{field}" {data_type}')
        conn.execute(
            "CREATE TABLE IF NOT EXISTS demon_event_lifecycle_operations ("
            "operation_id TEXT PRIMARY KEY,payload TEXT NOT NULL,result_json TEXT NOT NULL,created_at TEXT NOT NULL)"
        )

    @staticmethod
    def _read_state(conn, event_key: str) -> dict | None:
        fields = ",".join(f'"{field}"' for field in STATE_FIELDS)
        row = conn.execute(f"SELECT {fields} FROM world_event_state WHERE user_id=%s", (event_key,)).fetchone()
        if row is None:
            return None
        return {field: _decode(field, row[index]) for index, field in enumerate(STATE_FIELDS)}

    @staticmethod
    def _write_state(conn, event_key: str, state: dict) -> None:
        assignments = ",".join(f'"{field}"=%s' for field in STATE_FIELDS)
        values = [_encode(field, state.get(field)) for field in STATE_FIELDS]
        changed = conn.execute(f"UPDATE world_event_state SET {assignments} WHERE user_id=%s", (*values, event_key))
        if changed.rowcount == 0:
            fields = ",".join(["user_id", *[f'"{field}"' for field in STATE_FIELDS]])
            marks = ",".join("%s" for _ in range(len(STATE_FIELDS) + 1))
            conn.execute(f"INSERT INTO world_event_state ({fields}) VALUES ({marks})", (event_key, *values))

    @classmethod
    def _verify_state(cls, conn, event_key: str, expected: dict) -> None:
        if cls._read_state(conn, event_key) != expected:
            raise RuntimeError("demon lifecycle state verification failed")

    def replay(self, operation_id: str) -> DemonEventLifecycleResult | None:
        conn = db_backend.connect(self.player_db)
        try:
            self._ensure_schema(conn)
            row = conn.execute(
                "SELECT result_json FROM demon_event_lifecycle_operations WHERE operation_id=%s", (str(operation_id),),
            ).fetchone()
            return None if row is None else DemonEventLifecycleResult(**json.loads(str(row[0])))
        finally:
            conn.close()

    def transition(self, operation_id, event_key, action, expected_state, target_state):
        operation_id, action = str(operation_id).strip(), str(action).strip()
        if not operation_id or action not in {"auto_start", "manual_start", "auto_finish", "manual_finish"}:
            raise ValueError("invalid lifecycle operation")
        expected = None if expected_state is None else {field: _decode(field, expected_state.get(field)) for field in STATE_FIELDS}
        target = {field: _decode(field, target_state.get(field)) for field in STATE_FIELDS}
        payload = json.dumps(
            {"event_key": str(event_key), "action": action, "expected_state": expected, "target_state": target},
            ensure_ascii=False, sort_keys=True, separators=(",", ":"),
        )
        conn = db_backend.connect(self.player_db)
        try:
            self._ensure_schema(conn)
            conn.execute("BEGIN IMMEDIATE")
            previous = conn.execute(
                "SELECT payload,result_json FROM demon_event_lifecycle_operations WHERE operation_id=%s", (operation_id,),
            ).fetchone()
            if previous:
                if str(previous[0]) != payload:
                    conn.rollback()
                    return DemonEventLifecycleResult("operation_conflict", action)
                conn.commit()
                return DemonEventLifecycleResult(**json.loads(str(previous[1])))
            current = self._read_state(conn, str(event_key))
            first_start = current is None and action.endswith("start") and expected is not None
            first_start = first_start and expected.get("status") == "idle" and not expected.get("event_id")
            if current != expected and not first_start:
                conn.rollback()
                return DemonEventLifecycleResult("state_changed", action, current)
            if action.endswith("start"):
                valid = target.get("status") == "active" and int(target.get("active") or 0) == 1 and bool(target.get("event_id"))
                valid = valid and not (current and current.get("status") == "active")
            else:
                valid = current is not None and current.get("status") == "active"
                valid = valid and target.get("event_id") == current.get("event_id")
                valid = valid and target.get("status") == "finished" and int(target.get("active") or 0) == 0
            if not valid:
                conn.rollback()
                return DemonEventLifecycleResult("invalid_transition", action, current)
            self._write_state(conn, str(event_key), target)
            self._verify_state(conn, str(event_key), target)
            result = DemonEventLifecycleResult("applied", action, target)
            conn.execute(
                "INSERT INTO demon_event_lifecycle_operations VALUES (%s,%s,%s,CURRENT_TIMESTAMP)",
                (operation_id, payload, json.dumps(asdict(result), ensure_ascii=False, sort_keys=True)),
            )
            conn.commit()
            return result
        except Exception:
            conn.rollback()
            raise
        finally:
            conn.close()


__all__ = ["DemonEventLifecycleResult", "DemonEventLifecycleService"]
