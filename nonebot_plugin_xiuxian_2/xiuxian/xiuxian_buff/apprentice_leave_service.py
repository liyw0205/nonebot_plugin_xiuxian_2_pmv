from __future__ import annotations

import json
from contextlib import closing
from dataclasses import dataclass
from pathlib import Path
from threading import RLock

from ..xiuxian_utils import db_backend
from .relation_transaction_utils import append_mentor_history, get_json_field, increment_stat, set_field


@dataclass(frozen=True)
class ApprenticeLeaveResult:
    status: str
    apprentice_cd_until: str
    pair_rebind_until: str

    @property
    def succeeded(self) -> bool:
        return self.status in {"applied", "duplicate"}


class ApprenticeLeaveService:
    def __init__(self, game_database: str | Path, player_database: str | Path, lock: RLock | None = None):
        self._game_database = Path(game_database)
        self._player_database = Path(player_database)
        self._lock = lock or RLock()

    def apply(self, operation_id, mentor_id, apprentice_id, *, occurred_at, expected_apprentice_level,
              graduation_eligible, apprentice_cd_until, pair_rebind_until, history_limit,
              mentor_desc, apprentice_desc):
        operation_id, mentor_id, apprentice_id = str(operation_id).strip(), str(mentor_id), str(apprentice_id)
        values = (str(occurred_at), str(expected_apprentice_level), bool(graduation_eligible),
                  str(apprentice_cd_until), str(pair_rebind_until), int(history_limit))
        if not operation_id or mentor_id == apprentice_id or values[2] or values[5] <= 0:
            raise ValueError("invalid apprentice leave operation")
        payload = json.dumps([mentor_id, apprentice_id, *values, mentor_desc, apprentice_desc],
                             ensure_ascii=False, separators=(",", ":"))
        result = lambda status: ApprenticeLeaveResult(status, values[3], values[4])
        with self._lock, closing(db_backend.connect(self._game_database)) as conn:
            attached = False
            try:
                conn.execute("ATTACH DATABASE %s AS player_data", (str(self._player_database),)); attached = True
                conn.execute("BEGIN IMMEDIATE")
                conn.execute("CREATE TABLE IF NOT EXISTS apprentice_leave_operations (operation_id TEXT PRIMARY KEY,payload TEXT NOT NULL,created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP)")
                previous = conn.execute("SELECT payload FROM apprentice_leave_operations WHERE operation_id=%s", (operation_id,)).fetchone()
                if previous is not None:
                    conn.rollback(); return result("duplicate" if str(previous[0]) == payload else "operation_conflict")
                level = conn.execute("SELECT level FROM user_xiuxian WHERE user_id=%s", (apprentice_id,)).fetchone()
                apprentices = [str(v) for v in get_json_field(conn, "mentor", mentor_id, "apprentice_ids", [])]
                parent = conn.execute("SELECT mentor_id FROM player_data.mentor WHERE user_id=%s", (apprentice_id,)).fetchone()
                if level is None or str(level[0]) != values[1] or apprentice_id not in apprentices or parent is None or str(parent[0]) != mentor_id:
                    conn.rollback(); return result("state_changed")
                set_field(conn, "mentor", mentor_id, "apprentice_ids", [v for v in apprentices if v != apprentice_id])
                set_field(conn, "mentor", apprentice_id, "mentor_id", None)
                set_field(conn, "mentor", apprentice_id, "bind_time", None)
                set_field(conn, "mentor", apprentice_id, "breakthrough_reward_count", 0, "INTEGER")
                set_field(conn, "mentor", apprentice_id, "apprentice_cd_until", values[3])
                rebind = get_json_field(conn, "mentor", apprentice_id, "mentor_rebind_cd", {})
                rebind[mentor_id] = values[4]
                set_field(conn, "mentor", apprentice_id, "mentor_rebind_cd", rebind)
                increment_stat(conn, apprentice_id, "离开师门次数", 1)
                append_mentor_history(conn, mentor_id, "leave", apprentice_id, mentor_desc, values[5])
                append_mentor_history(conn, apprentice_id, "leave", mentor_id, apprentice_desc, values[5])
                conn.execute("INSERT INTO apprentice_leave_operations (operation_id,payload) VALUES (%s,%s)", (operation_id, payload))
                conn.commit(); return result("applied")
            except Exception:
                conn.rollback(); raise
            finally:
                if attached:
                    try: conn.execute("DETACH DATABASE player_data")
                    except Exception: pass


__all__ = ["ApprenticeLeaveResult", "ApprenticeLeaveService"]
