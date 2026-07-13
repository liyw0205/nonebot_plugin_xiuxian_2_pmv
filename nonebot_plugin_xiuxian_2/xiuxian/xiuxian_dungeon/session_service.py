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
                conn.execute("CREATE TABLE IF NOT EXISTS dungeon_session_operations (operation_id TEXT PRIMARY KEY, payload TEXT NOT NULL, result_status TEXT NOT NULL, dungeon_status TEXT NOT NULL, created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP)")
                previous = conn.execute("SELECT payload,result_status,dungeon_status FROM dungeon_session_operations WHERE operation_id=%s", (operation_id,)).fetchone()
                if previous:
                    conn.rollback()
                    return DungeonSessionResult("duplicate" if str(previous[0]) == payload else "state_changed", str(previous[2]))
                row = conn.execute("SELECT dungeon_id,dungeon_status,current_layer,total_layers,last_reset_date FROM player_dungeon_status WHERE user_id=%s", (user_id,)).fetchone()
                if row is None:
                    conn.rollback()
                    return DungeonSessionResult("state_changed")
                current = {"dungeon_id": str(row[0]), "dungeon_status": str(row[1]), "current_layer": int(row[2]), "total_layers": int(row[3]), "last_reset_date": str(row[4])}
                normalized_expected = {"dungeon_id": str(expected["dungeon_id"]), "dungeon_status": str(expected["dungeon_status"]), "current_layer": int(expected["current_layer"]), "total_layers": int(expected["total_layers"]), "last_reset_date": str(expected["last_reset_date"])}
                if current != normalized_expected or current["dungeon_id"] != str(dungeon["dungeon_id"]) or current["last_reset_date"] != str(dungeon["date"]):
                    conn.rollback()
                    return DungeonSessionResult("state_changed", current["dungeon_status"])
                if current["dungeon_status"] == "completed":
                    conn.rollback()
                    return DungeonSessionResult("completed", "completed")
                if action == "exit" and current["dungeon_status"] != "exploring":
                    conn.rollback()
                    return DungeonSessionResult("not_exploring", current["dungeon_status"])
                new_status = "exploring" if action == "enter" else "exited"
                conn.execute("UPDATE player_dungeon_status SET dungeon_status=%s WHERE user_id=%s", (new_status, user_id))
                conn.execute("INSERT INTO dungeon_session_operations VALUES (%s,%s,%s,%s,CURRENT_TIMESTAMP)", (operation_id, payload, "applied", new_status))
                conn.commit()
                return DungeonSessionResult("applied", new_status)
            except Exception:
                conn.rollback()
                raise


__all__ = ["DungeonSessionResult", "DungeonSessionService"]
