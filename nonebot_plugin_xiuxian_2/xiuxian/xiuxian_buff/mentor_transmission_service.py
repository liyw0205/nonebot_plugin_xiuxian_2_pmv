from __future__ import annotations

import json
from contextlib import closing
from dataclasses import dataclass
from pathlib import Path
from threading import RLock

from ..xiuxian_utils import db_backend
from .relation_transaction_utils import append_mentor_history, get_json_field, increment_stat


@dataclass(frozen=True)
class MentorTransmissionResult:
    status: str
    reward_exp: int

    @property
    def succeeded(self) -> bool:
        return self.status in {"applied", "duplicate"}


class MentorTransmissionService:
    def __init__(self, game_database: str | Path, player_database: str | Path, lock: RLock | None = None):
        self._game_database = Path(game_database); self._player_database = Path(player_database); self._lock = lock or RLock()

    def apply(self, operation_id, mentor_id, apprentice_id, *, expected_apprentice_exp, reward_exp, power, hp, mp, atk, mentor_used, apprentice_used, daily_limit, history_limit, mentor_desc, apprentice_desc):
        operation_id, mentor_id, apprentice_id = str(operation_id).strip(), str(mentor_id), str(apprentice_id)
        values = tuple(int(v) for v in (expected_apprentice_exp, reward_exp, power, hp, mp, atk, mentor_used, apprentice_used, daily_limit, history_limit))
        if not operation_id or values[1] <= 0 or values[6] >= values[8] or values[7] >= values[8]:
            raise ValueError("invalid mentor transmission operation")
        payload = json.dumps([mentor_id, apprentice_id, *values, mentor_desc, apprentice_desc], separators=(",", ":"), ensure_ascii=False)
        result = lambda status: MentorTransmissionResult(status, values[1])
        with self._lock, closing(db_backend.connect(self._game_database)) as conn:
            attached = False
            try:
                conn.execute("ATTACH DATABASE %s AS player_data", (str(self._player_database),)); attached = True; conn.execute("BEGIN IMMEDIATE")
                conn.execute("CREATE TABLE IF NOT EXISTS mentor_transmission_operations (operation_id TEXT PRIMARY KEY,payload TEXT NOT NULL,reward_exp INTEGER NOT NULL,created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP)")
                previous = conn.execute("SELECT payload,reward_exp FROM mentor_transmission_operations WHERE operation_id=%s", (operation_id,)).fetchone()
                if previous is not None:
                    conn.rollback()
                    return MentorTransmissionResult("duplicate" if str(previous[0]) == payload else "operation_conflict", int(previous[1]))
                apprentices = [str(v) for v in get_json_field(conn, "mentor", mentor_id, "apprentice_ids", [])]
                apprentice_row = conn.execute("SELECT mentor_id FROM player_data.mentor WHERE user_id=%s", (apprentice_id,)).fetchone()
                if apprentice_id not in apprentices or apprentice_row is None or str(apprentice_row[0]) != mentor_id:
                    conn.rollback(); return result("state_changed")
                changed = conn.execute(
                    "UPDATE user_xiuxian SET exp=exp+%s,power=%s,hp=%s,mp=%s,atk=%s WHERE user_id=%s AND exp=%s",
                    (values[1], values[2], values[3], values[4], values[5], apprentice_id, values[0]),
                )
                if changed.rowcount != 1:
                    conn.rollback(); return result("state_changed")
                increment_stat(conn, mentor_id, "师徒传功次数", 1)
                increment_stat(conn, apprentice_id, "接受传功次数", 1)
                increment_stat(conn, apprentice_id, "传功获得修为", values[1])
                append_mentor_history(conn, mentor_id, "transmission", apprentice_id, mentor_desc, values[9])
                append_mentor_history(conn, apprentice_id, "transmission", mentor_id, apprentice_desc, values[9])
                conn.execute("INSERT INTO mentor_transmission_operations (operation_id,payload,reward_exp) VALUES (%s,%s,%s)", (operation_id, payload, values[1]))
                conn.commit(); return result("applied")
            except Exception:
                conn.rollback(); raise
            finally:
                if attached:
                    try: conn.execute("DETACH DATABASE player_data")
                    except Exception: pass


__all__ = ["MentorTransmissionResult", "MentorTransmissionService"]
