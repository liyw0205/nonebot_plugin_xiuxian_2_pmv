from __future__ import annotations

import json
from contextlib import closing
from dataclasses import dataclass
from pathlib import Path
from threading import RLock

from ..xiuxian_utils import db_backend


@dataclass(frozen=True)
class RiftTerminationResult:
    status: str

    @property
    def succeeded(self) -> bool:
        return self.status in {"applied", "duplicate"}


class RiftTerminationService:
    """Atomically abandon an active rift and release the user's busy state."""

    def __init__(self, database: str | Path, lock: RLock | None = None) -> None:
        self._database = Path(database)
        self._lock = lock or RLock()

    def terminate(self, operation_id, user_id, rift_data) -> RiftTerminationResult:
        operation_id = str(operation_id).strip()
        user_id = str(user_id)
        snapshot = json.dumps(rift_data, ensure_ascii=False, sort_keys=True)
        if not operation_id or not user_id:
            raise ValueError("operation_id and user_id are required")
        payload = json.dumps([user_id, snapshot], ensure_ascii=True)

        with self._lock, closing(db_backend.connect(self._database)) as conn:
            try:
                conn.execute("BEGIN IMMEDIATE")
                conn.execute(
                    "CREATE TABLE IF NOT EXISTS rift_termination_operations "
                    "(operation_id TEXT PRIMARY KEY,payload TEXT NOT NULL,created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP)"
                )
                previous = conn.execute(
                    "SELECT payload FROM rift_termination_operations WHERE operation_id=%s", (operation_id,)
                ).fetchone()
                if previous is not None:
                    conn.rollback()
                    return RiftTerminationResult("duplicate" if str(previous[0]) == payload else "state_changed")
                entry = conn.execute(
                    "SELECT rift_data,status FROM rift_entries WHERE user_id=%s", (user_id,)
                ).fetchone()
                if entry is None or str(entry[1]) != "active":
                    conn.rollback()
                    return RiftTerminationResult("not_active")
                if json.loads(str(entry[0])) != json.loads(snapshot):
                    conn.rollback()
                    return RiftTerminationResult("state_changed")
                cd = conn.execute("SELECT COALESCE(type,0) FROM user_cd WHERE user_id=%s", (user_id,)).fetchone()
                if cd is None or int(cd[0]) != 3:
                    conn.rollback()
                    return RiftTerminationResult("state_changed")
                conn.execute("UPDATE rift_entries SET status='terminated' WHERE user_id=%s", (user_id,))
                conn.execute(
                    "UPDATE user_cd SET type=0,create_time=0,scheduled_time=NULL WHERE user_id=%s AND type=3", (user_id,)
                )
                conn.execute(
                    "INSERT INTO rift_termination_operations VALUES (%s,%s,CURRENT_TIMESTAMP)",
                    (operation_id, payload),
                )
                conn.commit()
                return RiftTerminationResult("applied")
            except Exception:
                conn.rollback()
                raise


__all__ = ["RiftTerminationResult", "RiftTerminationService"]
