from __future__ import annotations

import json
from contextlib import closing
from dataclasses import dataclass
from pathlib import Path
from threading import RLock

from ..xiuxian_utils import db_backend
from .relation_transaction_utils import append_mentor_history, get_json_field, increment_stat, set_field


@dataclass(frozen=True)
class MentorBreakthroughRewardResult:
    status: str
    reward_exp: int
    reward_count: int

    @property
    def succeeded(self) -> bool:
        return self.status in {"applied", "duplicate"}


class MentorBreakthroughRewardService:
    def __init__(self, game_database: str | Path, player_database: str | Path, lock: RLock | None = None):
        self._game_database = Path(game_database)
        self._player_database = Path(player_database)
        self._lock = lock or RLock()

    def apply(self, operation_id, mentor_id, apprentice_id, new_level, business_event_id, *,
              expected_mentor_exp, expected_apprentice_exp, expected_reward_count, reward_limit,
              reward_exp, max_mentor_exp, mentor_power, history_limit, mentor_desc, apprentice_desc):
        operation_id, mentor_id, apprentice_id = str(operation_id).strip(), str(mentor_id), str(apprentice_id)
        new_level, business_event_id = str(new_level), str(business_event_id).strip()
        values = tuple(int(v) for v in (expected_mentor_exp, expected_apprentice_exp, expected_reward_count,
                                        reward_limit, reward_exp, max_mentor_exp, mentor_power, history_limit))
        if not operation_id or not business_event_id or mentor_id == apprentice_id or values[4] <= 0 or values[2] < 0 or values[2] >= values[3]:
            raise ValueError("invalid mentor breakthrough reward")
        if values[0] + values[4] > values[5]:
            raise ValueError("mentor exp cap exceeded")
        payload = json.dumps([mentor_id, apprentice_id, new_level, business_event_id, *values, mentor_desc, apprentice_desc],
                             ensure_ascii=False, separators=(",", ":"))
        result = lambda status, count=values[2] + 1: MentorBreakthroughRewardResult(status, values[4], count)
        with self._lock, closing(db_backend.connect(self._game_database)) as conn:
            attached = False
            try:
                conn.execute("ATTACH DATABASE %s AS player_data", (str(self._player_database),)); attached = True
                conn.execute("BEGIN IMMEDIATE")
                conn.execute("CREATE TABLE IF NOT EXISTS mentor_breakthrough_reward_operations (operation_id TEXT PRIMARY KEY,business_event_id TEXT UNIQUE NOT NULL,payload TEXT NOT NULL,reward_exp INTEGER NOT NULL,reward_count INTEGER NOT NULL,created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP)")
                previous = conn.execute("SELECT payload,reward_exp,reward_count FROM mentor_breakthrough_reward_operations WHERE operation_id=%s", (operation_id,)).fetchone()
                if previous is not None:
                    conn.rollback()
                    return MentorBreakthroughRewardResult("duplicate" if str(previous[0]) == payload else "operation_conflict", int(previous[1]), int(previous[2]))
                event = conn.execute("SELECT payload,reward_exp,reward_count FROM mentor_breakthrough_reward_operations WHERE business_event_id=%s", (business_event_id,)).fetchone()
                if event is not None:
                    conn.rollback(); return MentorBreakthroughRewardResult("event_duplicate", int(event[1]), int(event[2]))
                apprentices = [str(v) for v in get_json_field(conn, "mentor", mentor_id, "apprentice_ids", [])]
                parent = conn.execute("SELECT mentor_id FROM player_data.mentor WHERE user_id=%s", (apprentice_id,)).fetchone()
                count = get_json_field(conn, "mentor", apprentice_id, "breakthrough_reward_count", 0)
                users = conn.execute("SELECT user_id,exp FROM user_xiuxian WHERE user_id IN (%s,%s)", (mentor_id, apprentice_id)).fetchall()
                exps = {str(row[0]): int(row[1]) for row in users}
                if apprentice_id not in apprentices or parent is None or str(parent[0]) != mentor_id or int(count or 0) != values[2] or exps != {mentor_id: values[0], apprentice_id: values[1]}:
                    conn.rollback(); return result("state_changed")
                changed = conn.execute("UPDATE user_xiuxian SET exp=exp+%s,power=%s WHERE user_id=%s AND exp=%s AND exp+%s<=%s",
                                       (values[4], values[6], mentor_id, values[0], values[4], values[5]))
                if changed.rowcount != 1:
                    conn.rollback(); return result("state_changed")
                set_field(conn, "mentor", apprentice_id, "breakthrough_reward_count", values[2] + 1, "INTEGER")
                increment_stat(conn, mentor_id, "师父突破返修", values[4])
                increment_stat(conn, apprentice_id, "徒弟突破回馈", values[4])
                append_mentor_history(conn, mentor_id, "breakthrough_reward", apprentice_id, mentor_desc, values[7])
                append_mentor_history(conn, apprentice_id, "breakthrough_reward", mentor_id, apprentice_desc, values[7])
                conn.execute("INSERT INTO mentor_breakthrough_reward_operations (operation_id,business_event_id,payload,reward_exp,reward_count) VALUES (%s,%s,%s,%s,%s)",
                             (operation_id, business_event_id, payload, values[4], values[2] + 1))
                conn.commit(); return result("applied")
            except Exception:
                conn.rollback(); raise
            finally:
                if attached:
                    try: conn.execute("DETACH DATABASE player_data")
                    except Exception: pass


__all__ = ["MentorBreakthroughRewardResult", "MentorBreakthroughRewardService"]
