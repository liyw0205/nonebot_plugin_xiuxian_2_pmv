from __future__ import annotations

import json
from dataclasses import asdict, dataclass
from datetime import datetime
from pathlib import Path

from ..xiuxian_utils import db_backend
from .demon_wave_refresh_service import INTEGER_FIELDS, STATE_FIELDS, _decode, _encode


@dataclass(frozen=True)
class SpiritVeinLifecycleResult:
    status: str
    action: str = ""
    state: dict | None = None

    @property
    def succeeded(self) -> bool:
        return self.status not in {"operation_conflict", "state_changed", "invalid_transition"}


class SpiritVeinLifecycleService:
    _ACTIONS = {
        "auto_start",
        "auto_skip",
        "auto_miss",
        "manual_start",
        "manual_start_skip",
        "manual_finish",
        "manual_finish_skip",
        "expire",
    }
    _NOOP_ACTIONS = {
        "auto_skip",
        "auto_miss",
        "manual_start_skip",
        "manual_finish_skip",
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

    def __init__(self, player_db: str | Path) -> None:
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
            "CREATE TABLE IF NOT EXISTS spirit_vein_lifecycle_operations("
            "operation_id TEXT PRIMARY KEY,payload TEXT NOT NULL,"
            "result_json TEXT NOT NULL,created_at TEXT NOT NULL)"
        )

    @staticmethod
    def _normalize(state: dict | None) -> dict | None:
        if state is None:
            return None
        return {field: _decode(field, state.get(field)) for field in STATE_FIELDS}

    @staticmethod
    def _read_state(conn, event_key: str) -> dict | None:
        fields = ",".join(f'"{field}"' for field in STATE_FIELDS)
        row = conn.execute(
            f"SELECT {fields} FROM world_event_state WHERE user_id=%s",
            (event_key,),
        ).fetchone()
        if row is None:
            return None
        return {
            field: _decode(field, row[index])
            for index, field in enumerate(STATE_FIELDS)
        }

    @staticmethod
    def _write_state(conn, event_key: str, state: dict) -> None:
        assignments = ",".join(f'"{field}"=%s' for field in STATE_FIELDS)
        values = [_encode(field, state.get(field)) for field in STATE_FIELDS]
        changed = conn.execute(
            f"UPDATE world_event_state SET {assignments} WHERE user_id=%s",
            (*values, event_key),
        )
        if changed.rowcount == 0:
            fields = ",".join(["user_id", *[f'"{field}"' for field in STATE_FIELDS]])
            marks = ",".join("%s" for _ in range(len(STATE_FIELDS) + 1))
            conn.execute(
                f"INSERT INTO world_event_state ({fields}) VALUES ({marks})",
                (event_key, *values),
            )

    @classmethod
    def _verify_state(cls, conn, event_key: str, expected: dict) -> None:
        if cls._read_state(conn, event_key) != expected:
            raise RuntimeError("spirit vein lifecycle state verification failed")

    @staticmethod
    def _parse_time(value) -> datetime | None:
        try:
            return datetime.fromisoformat(str(value)) if value else None
        except ValueError:
            return None

    @classmethod
    def _valid_transition(cls, action: str, current: dict | None, target: dict) -> bool:
        current_state = current or target
        if action in cls._NOOP_ACTIONS:
            if target != current_state:
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
        conn = db_backend.connect(self.player_db)
        try:
            self._ensure_schema(conn)
            row = conn.execute(
                "SELECT result_json FROM spirit_vein_lifecycle_operations "
                "WHERE operation_id=%s",
                (str(operation_id),),
            ).fetchone()
            if row is None:
                return None
            return SpiritVeinLifecycleResult(**json.loads(str(row[0])))
        finally:
            conn.close()

    def transition(
        self,
        operation_id,
        event_key,
        action,
        expected_state,
        target_state,
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

        conn = db_backend.connect(self.player_db)
        try:
            self._ensure_schema(conn)
            conn.execute("BEGIN IMMEDIATE")
            previous = conn.execute(
                "SELECT payload,result_json FROM spirit_vein_lifecycle_operations "
                "WHERE operation_id=%s",
                (operation_id,),
            ).fetchone()
            if previous is not None:
                if str(previous[0]) != payload:
                    conn.rollback()
                    return SpiritVeinLifecycleResult("operation_conflict", action)
                result = SpiritVeinLifecycleResult(**json.loads(str(previous[1])))
                conn.commit()
                return result

            current = self._read_state(conn, event_key)
            first_idle_state = (
                current is None
                and expected is not None
                and expected.get("status") == "idle"
                and not expected.get("event_id")
            )
            if current != expected and not first_idle_state:
                conn.rollback()
                return SpiritVeinLifecycleResult("state_changed", action, current)
            effective_current = current if current is not None else expected
            if not self._valid_transition(action, effective_current, target):
                conn.rollback()
                return SpiritVeinLifecycleResult(
                    "invalid_transition",
                    action,
                    effective_current,
                )

            if action not in self._NOOP_ACTIONS:
                self._write_state(conn, event_key, target)
                self._verify_state(conn, event_key, target)
            result = SpiritVeinLifecycleResult(
                self._RESULT_STATUS[action],
                action,
                target,
            )
            conn.execute(
                "INSERT INTO spirit_vein_lifecycle_operations "
                "VALUES(%s,%s,%s,CURRENT_TIMESTAMP)",
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
            conn.commit()
            return result
        except Exception:
            conn.rollback()
            raise
        finally:
            conn.close()


__all__ = ["SpiritVeinLifecycleResult", "SpiritVeinLifecycleService"]
