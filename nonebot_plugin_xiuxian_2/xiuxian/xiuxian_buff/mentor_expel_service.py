from __future__ import annotations

import json
from contextlib import closing
from dataclasses import dataclass
from pathlib import Path
from threading import RLock

from ..xiuxian_utils import db_backend
from .relation_transaction_utils import append_mentor_history, get_json_field, increment_stat, set_field


@dataclass(frozen=True)
class MentorExpelResult:
    status: str
    mentor_cd_until: str
    apprentice_cd_until: str
    pair_rebind_until: str

    @property
    def succeeded(self) -> bool:
        return self.status in {"applied", "duplicate"}


class MentorExpelService:
    def __init__(self, game_database: str | Path, player_database: str | Path, lock: RLock | None = None):
        self._game_database = Path(game_database)
        self._player_database = Path(player_database)
        self._lock = lock or RLock()

    def apply(self, operation_id, mentor_id, apprentice_id, *, occurred_at, mentor_cd_until,
              apprentice_cd_until, pair_rebind_until, history_limit, mentor_desc, apprentice_desc):
        operation_id, mentor_id, apprentice_id = str(operation_id).strip(), str(mentor_id), str(apprentice_id)
        values = tuple(str(v) for v in (occurred_at, mentor_cd_until, apprentice_cd_until, pair_rebind_until))
        history_limit = int(history_limit)
        if not operation_id or mentor_id == apprentice_id or history_limit <= 0:
            raise ValueError("invalid mentor expel operation")
        payload = json.dumps([mentor_id, apprentice_id, *values, history_limit, mentor_desc, apprentice_desc],
                             ensure_ascii=False, separators=(",", ":"))
        result = lambda status: MentorExpelResult(status, values[1], values[2], values[3])
        with self._lock, closing(db_backend.connect(self._game_database)) as conn:
            attached = False
            try:
                conn.execute("ATTACH DATABASE %s AS player_data", (str(self._player_database),)); attached = True
                conn.execute("BEGIN IMMEDIATE")
                conn.execute("CREATE TABLE IF NOT EXISTS mentor_expel_operations (operation_id TEXT PRIMARY KEY,payload TEXT NOT NULL,created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP)")
                previous = conn.execute("SELECT payload FROM mentor_expel_operations WHERE operation_id=%s", (operation_id,)).fetchone()
                if previous is not None:
                    conn.rollback(); return result("duplicate" if str(previous[0]) == payload else "operation_conflict")
                apprentices = [str(v) for v in get_json_field(conn, "mentor", mentor_id, "apprentice_ids", [])]
                parent = conn.execute("SELECT mentor_id FROM player_data.mentor WHERE user_id=%s", (apprentice_id,)).fetchone()
                if apprentice_id not in apprentices or parent is None or str(parent[0]) != mentor_id:
                    conn.rollback(); return result("state_changed")
                set_field(conn, "mentor", mentor_id, "apprentice_ids", [v for v in apprentices if v != apprentice_id])
                set_field(conn, "mentor", mentor_id, "mentor_cd_until", values[1])
                set_field(conn, "mentor", apprentice_id, "mentor_id", None)
                set_field(conn, "mentor", apprentice_id, "bind_time", None)
                set_field(conn, "mentor", apprentice_id, "breakthrough_reward_count", 0, "INTEGER")
                set_field(conn, "mentor", apprentice_id, "apprentice_cd_until", values[2])
                rebind = get_json_field(conn, "mentor", apprentice_id, "mentor_rebind_cd", {})
                rebind[mentor_id] = values[3]
                set_field(conn, "mentor", apprentice_id, "mentor_rebind_cd", rebind)
                increment_stat(conn, mentor_id, "逐出徒弟次数", 1)
                increment_stat(conn, apprentice_id, "被逐出师门次数", 1)
                append_mentor_history(conn, mentor_id, "expel", apprentice_id, mentor_desc, history_limit)
                append_mentor_history(conn, apprentice_id, "expel", mentor_id, apprentice_desc, history_limit)
                conn.execute("INSERT INTO mentor_expel_operations (operation_id,payload) VALUES (%s,%s)", (operation_id, payload))
                conn.commit(); return result("applied")
            except Exception:
                conn.rollback(); raise
            finally:
                if attached:
                    try: conn.execute("DETACH DATABASE player_data")
                    except Exception: pass


__all__ = ["MentorExpelResult", "MentorExpelService"]
