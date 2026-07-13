from __future__ import annotations

import json
from contextlib import closing
from dataclasses import dataclass
from pathlib import Path
from threading import RLock

from ..xiuxian_utils import db_backend


@dataclass(frozen=True)
class WorkAbortResult:
    status: str
    penalty: int = 0
    stone_remaining: int = 0

    @property
    def succeeded(self):
        return self.status in {"applied", "duplicate"}


class WorkAbortService:
    """Apply the active-work abort penalty and clear its authoritative state atomically."""

    def __init__(self, database: str | Path, lock: RLock | None = None):
        self.database = Path(database)
        self.lock = lock or RLock()

    def abort(self, operation_id, user_id, expected_work, expected_stone, penalty):
        operation_id, user_id = str(operation_id).strip(), str(user_id)
        expected = dict(expected_work)
        expected_stone, penalty = map(int, (expected_stone, penalty))
        if not operation_id or penalty < 0:
            raise ValueError("invalid work abort request")
        payload = json.dumps([user_id, expected, expected_stone, penalty], ensure_ascii=True, sort_keys=True, default=str)
        with self.lock, closing(db_backend.connect(self.database)) as conn:
            try:
                conn.execute("BEGIN IMMEDIATE")
                conn.execute("CREATE TABLE IF NOT EXISTS work_abort_operations(operation_id TEXT PRIMARY KEY,payload TEXT NOT NULL,penalty INTEGER NOT NULL,stone_remaining INTEGER NOT NULL)")
                old = conn.execute("SELECT payload,penalty,stone_remaining FROM work_abort_operations WHERE operation_id=%s", (operation_id,)).fetchone()
                if old:
                    conn.rollback()
                    return WorkAbortResult("duplicate" if old[0] == payload else "operation_conflict", int(old[1]), int(old[2]))
                user = conn.execute("SELECT COALESCE(stone,0) FROM user_xiuxian WHERE user_id=%s", (user_id,)).fetchone()
                work = conn.execute("SELECT COALESCE(type,0),create_time,scheduled_time FROM user_cd WHERE user_id=%s", (user_id,)).fetchone()
                if user is None or work is None:
                    conn.rollback(); return WorkAbortResult("user_missing")
                if int(user[0]) != expected_stone or int(work[0]) != 2 or tuple(map(str, work[1:])) != (str(expected.get("create_time")), str(expected.get("scheduled_time"))):
                    conn.rollback(); return WorkAbortResult("state_changed")
                applied_penalty = min(penalty, expected_stone)
                remaining = expected_stone - applied_penalty
                changed = conn.execute("UPDATE user_xiuxian SET stone=%s WHERE user_id=%s AND stone=%s", (remaining, user_id, expected_stone))
                if changed.rowcount != 1:
                    conn.rollback(); return WorkAbortResult("state_changed")
                conn.execute("UPDATE user_cd SET type=0,create_time=0,scheduled_time=NULL WHERE user_id=%s AND type=2", (user_id,))
                conn.execute("CREATE TABLE IF NOT EXISTS work_active_snapshots(user_id TEXT PRIMARY KEY,snapshot TEXT NOT NULL,updated_at TEXT NOT NULL)")
                conn.execute("DELETE FROM work_active_snapshots WHERE user_id=%s", (user_id,))
                conn.execute("INSERT INTO work_abort_operations VALUES (%s,%s,%s,%s)", (operation_id, payload, applied_penalty, remaining))
                conn.commit(); return WorkAbortResult("applied", applied_penalty, remaining)
            except Exception:
                conn.rollback(); raise


__all__ = ["WorkAbortResult", "WorkAbortService"]
