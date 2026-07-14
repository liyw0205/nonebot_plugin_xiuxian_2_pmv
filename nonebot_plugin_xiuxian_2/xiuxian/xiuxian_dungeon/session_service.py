from __future__ import annotations

import json
from contextlib import closing
from dataclasses import dataclass
from pathlib import Path
from threading import RLock

from ..xiuxian_utils import db_backend


@dataclass(frozen=True)
class DungeonSessionResult:
    status: str
    dungeon_status: str = ""


class DungeonSessionService:
    """Atomically enter or leave the current dungeon session."""

    def __init__(self, player_database: str | Path, lock: RLock | None = None) -> None:
        self._database = Path(player_database)
        self._lock = lock or RLock()

    def enter(self, operation_id, user_id, expected, dungeon) -> DungeonSessionResult:
        return self._transition(operation_id, user_id, expected, dungeon, "enter")

    def exit(self, operation_id, user_id, expected, dungeon) -> DungeonSessionResult:
        return self._transition(operation_id, user_id, expected, dungeon, "exit")

    @staticmethod
    def _ensure_schema(conn) -> None:
        conn.execute(
            "CREATE TABLE IF NOT EXISTS dungeon_session_operations ("
            "operation_id TEXT PRIMARY KEY,payload TEXT NOT NULL,"
            "result_status TEXT NOT NULL,dungeon_status TEXT NOT NULL,"
            "created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP)"
        )

    def operation_result(
        self, operation_id, user_id, action
    ) -> DungeonSessionResult | None:
        """Replay a recorded transition before reading the current dungeon snapshot."""

        operation_id = str(operation_id).strip()
        user_id, action = str(user_id), str(action)
        if not operation_id or action not in {"enter", "exit"}:
            raise ValueError("valid operation is required")
        with self._lock, closing(db_backend.connect(self._database)) as conn:
            try:
                conn.execute("BEGIN IMMEDIATE")
                self._ensure_schema(conn)
                previous = conn.execute(
                    "SELECT payload,result_status,dungeon_status "
                    "FROM dungeon_session_operations WHERE operation_id=%s",
                    (operation_id,),
                ).fetchone()
                conn.commit()
            except Exception:
                conn.rollback()
                raise
        if previous is None:
            return None
        try:
            payload = json.loads(str(previous[0]))
        except (TypeError, ValueError, json.JSONDecodeError):
            return DungeonSessionResult("state_changed", str(previous[2]))
        if (
            not isinstance(payload, dict)
            or str(payload.get("user_id", "")) != user_id
            or str(payload.get("action", "")) != action
        ):
            return DungeonSessionResult("state_changed", str(previous[2]))
        result_status = str(previous[1])
        return DungeonSessionResult(
            "duplicate" if result_status == "applied" else result_status,
            str(previous[2]),
        )

    def _transition(self, operation_id, user_id, expected, dungeon, action) -> DungeonSessionResult:
        operation_id = str(operation_id).strip()
        user_id = str(user_id)
        expected = {str(k): expected[k] for k in expected}
        dungeon = {str(k): dungeon[k] for k in dungeon}
        if not operation_id or action not in {"enter", "exit"}:
            raise ValueError("valid operation is required")
        payload = json.dumps({"user_id": user_id, "dungeon": dungeon, "action": action}, ensure_ascii=True, sort_keys=True)
        with self._lock, closing(db_backend.connect(self._database)) as conn:
            try:
                conn.execute("BEGIN IMMEDIATE")
                self._ensure_schema(conn)
                previous = conn.execute("SELECT payload,result_status,dungeon_status FROM dungeon_session_operations WHERE operation_id=%s", (operation_id,)).fetchone()
                if previous:
                    conn.rollback()
                    if str(previous[0]) != payload:
                        return DungeonSessionResult("state_changed", str(previous[2]))
                    result_status = str(previous[1])
                    return DungeonSessionResult(
                        "duplicate" if result_status == "applied" else result_status,
                        str(previous[2]),
                    )

                def record_result(result_status: str, dungeon_status: str = ""):
                    conn.execute(
                        "INSERT INTO dungeon_session_operations VALUES "
                        "(%s,%s,%s,%s,CURRENT_TIMESTAMP)",
                        (operation_id, payload, result_status, dungeon_status),
                    )
                    conn.commit()
                    return DungeonSessionResult(result_status, dungeon_status)

                columns = [
                    "dungeon_id",
                    "dungeon_status",
                    "current_layer",
                    "total_layers",
                    "last_reset_date",
                ]
                available = set(conn.column_names("player_dungeon_status"))
                for optional in ("reset_generation", "reset_operation_id"):
                    if optional in available:
                        if optional not in expected:
                            return record_result("state_changed")
                        columns.append(optional)
                row = conn.execute(
                    "SELECT " + ",".join(columns)
                    + " FROM player_dungeon_status WHERE user_id=%s",
                    (user_id,),
                ).fetchone()
                if row is None:
                    return record_result("state_changed")
                current = dict(zip(columns, row))
                normalized_expected = {key: expected[key] for key in columns}
                for field in ("dungeon_id", "dungeon_status", "last_reset_date", "reset_operation_id"):
                    if field in current:
                        current[field] = str(current[field] or "")
                        normalized_expected[field] = str(normalized_expected[field] or "")
                for field in ("current_layer", "total_layers", "reset_generation"):
                    if field in current:
                        current[field] = int(current[field] or 0)
                        normalized_expected[field] = int(normalized_expected[field] or 0)
                if current != normalized_expected or current["dungeon_id"] != str(dungeon["dungeon_id"]) or current["last_reset_date"] != str(dungeon["date"]):
                    return record_result("state_changed", current["dungeon_status"])
                if current["dungeon_status"] == "completed":
                    return record_result("completed", "completed")
                if action == "exit" and current["dungeon_status"] != "exploring":
                    return record_result("not_exploring", current["dungeon_status"])
                new_status = "exploring" if action == "enter" else "exited"
                conn.execute("UPDATE player_dungeon_status SET dungeon_status=%s WHERE user_id=%s", (new_status, user_id))
                conn.execute("INSERT INTO dungeon_session_operations VALUES (%s,%s,%s,%s,CURRENT_TIMESTAMP)", (operation_id, payload, "applied", new_status))
                conn.commit()
                return DungeonSessionResult("applied", new_status)
            except Exception:
                conn.rollback()
                raise


__all__ = ["DungeonSessionResult", "DungeonSessionService"]
