from __future__ import annotations

import json
from contextlib import closing
from dataclasses import dataclass
from datetime import datetime, timedelta
from pathlib import Path
from threading import RLock

from ..xiuxian_utils import db_backend
from .relation_transaction_utils import append_mentor_history, get_json_field, increment_stat, set_field


@dataclass(frozen=True)
class MentorGraduationResult:
    status: str
    apprentice_stone: int
    mentor_stone: int

    @property
    def succeeded(self) -> bool:
        return self.status in {"applied", "duplicate"}


class MentorGraduationService:
    def __init__(self, game_database: str | Path, player_database: str | Path, lock: RLock | None = None):
        self._game_database = Path(game_database)
        self._player_database = Path(player_database)
        self._lock = lock or RLock()

    def apply(self, operation_id, mentor_id, apprentice_id, *, expected_mentor_stone, expected_apprentice_stone, apprentice_reward, mentor_reward, cooldown_days, history_limit, mentor_desc, apprentice_desc, apprentice_title_ids=(), mentor_title_ids=()):
        operation_id, mentor_id, apprentice_id = str(operation_id).strip(), str(mentor_id), str(apprentice_id)
        values = tuple(int(v) for v in (expected_mentor_stone, expected_apprentice_stone, apprentice_reward, mentor_reward, cooldown_days, history_limit))
        if not operation_id or values[2] < 0 or values[3] < 0:
            raise ValueError("invalid graduation operation")
        apprentice_title_ids = tuple(sorted(str(v) for v in apprentice_title_ids))
        mentor_title_ids = tuple(sorted(str(v) for v in mentor_title_ids))
        payload = json.dumps([mentor_id, apprentice_id, *values, mentor_desc, apprentice_desc, apprentice_title_ids, mentor_title_ids], separators=(",", ":"), ensure_ascii=False)
        result = lambda status: MentorGraduationResult(status, values[2], values[3])
        with self._lock, closing(db_backend.connect(self._game_database)) as conn:
            attached = False
            try:
                conn.execute("ATTACH DATABASE %s AS player_data", (str(self._player_database),))
                attached = True
                conn.execute("BEGIN IMMEDIATE")
                conn.execute(
                    "CREATE TABLE IF NOT EXISTS mentor_graduation_operations (operation_id TEXT PRIMARY KEY,"
                    "payload TEXT NOT NULL,apprentice_stone INTEGER NOT NULL,mentor_stone INTEGER NOT NULL,"
                    "created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP)"
                )
                previous = conn.execute("SELECT payload,apprentice_stone,mentor_stone FROM mentor_graduation_operations WHERE operation_id=%s", (operation_id,)).fetchone()
                if previous is not None:
                    conn.rollback()
                    return MentorGraduationResult("duplicate" if str(previous[0]) == payload else "operation_conflict", int(previous[1]), int(previous[2]))
                mentor_apprentices = [str(v) for v in get_json_field(conn, "mentor", mentor_id, "apprentice_ids", [])]
                apprentice_mentor = conn.execute("SELECT mentor_id FROM player_data.mentor WHERE user_id=%s", (apprentice_id,)).fetchone()
                if apprentice_id not in mentor_apprentices or apprentice_mentor is None or str(apprentice_mentor[0]) != mentor_id:
                    conn.rollback()
                    return result("state_changed")
                stones = conn.execute("SELECT user_id,stone FROM user_xiuxian WHERE user_id IN (%s,%s)", (mentor_id, apprentice_id)).fetchall()
                if {str(row[0]): int(row[1]) for row in stones} != {mentor_id: values[0], apprentice_id: values[1]}:
                    conn.rollback()
                    return result("state_changed")
                for uid, expected, reward in ((mentor_id, values[0], values[3]), (apprentice_id, values[1], values[2])):
                    if conn.execute("UPDATE user_xiuxian SET stone=stone+%s WHERE user_id=%s AND stone=%s", (reward, uid, expected)).rowcount != 1:
                        conn.rollback()
                        return result("state_changed")
                    increment_stat(conn, uid, "灵石获取", reward)
                increment_stat(conn, apprentice_id, "正常出师次数", 1)
                increment_stat(conn, mentor_id, "培养出师徒弟", 1)
                for uid, title_ids in ((apprentice_id, apprentice_title_ids), (mentor_id, mentor_title_ids)):
                    unlocked = {str(v) for v in get_json_field(conn, "title", uid, "unlocked", [])}
                    unlocked.update(title_ids)
                    set_field(conn, "title", uid, "unlocked", sorted(unlocked))
                set_field(conn, "mentor", mentor_id, "apprentice_ids", [uid for uid in mentor_apprentices if uid != apprentice_id])
                set_field(conn, "mentor", apprentice_id, "mentor_id", None)
                set_field(conn, "mentor", apprentice_id, "bind_time", None)
                set_field(conn, "mentor", apprentice_id, "breakthrough_reward_count", 0, "INTEGER")
                rebind = get_json_field(conn, "mentor", apprentice_id, "mentor_rebind_cd", {})
                rebind[mentor_id] = (datetime.now() + timedelta(days=values[4])).strftime("%Y-%m-%d %H:%M:%S")
                set_field(conn, "mentor", apprentice_id, "mentor_rebind_cd", rebind)
                append_mentor_history(conn, mentor_id, "graduate", apprentice_id, mentor_desc, values[5])
                append_mentor_history(conn, apprentice_id, "graduate", mentor_id, apprentice_desc, values[5])
                conn.execute("INSERT INTO mentor_graduation_operations (operation_id,payload,apprentice_stone,mentor_stone) VALUES (%s,%s,%s,%s)", (operation_id, payload, values[2], values[3]))
                conn.commit()
                return result("applied")
            except Exception:
                conn.rollback()
                raise
            finally:
                if attached:
                    try:
                        conn.execute("DETACH DATABASE player_data")
                    except Exception:
                        pass


__all__ = ["MentorGraduationResult", "MentorGraduationService"]
