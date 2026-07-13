from __future__ import annotations

import json
from contextlib import closing
from dataclasses import dataclass
from pathlib import Path
from threading import RLock

from ..xiuxian_utils import db_backend


def _dump(value) -> str:
    return json.dumps(value, ensure_ascii=True, sort_keys=True, separators=(",", ":"), default=str)


@dataclass(frozen=True)
class WorkAbortCleanupResult:
    status: str
    penalty: int = 0
    stone_remaining: int = 0

    @property
    def succeeded(self) -> bool:
        return self.status in {"applied", "duplicate"}


class WorkAbortCleanupService:
    """Close active or offered work together with its CD, assets and snapshots."""

    def __init__(self, database: str | Path, lock: RLock | None = None) -> None:
        self._database = Path(database)
        self._lock = lock or RLock()

    def cleanup(
        self,
        operation_id,
        user_id,
        reason,
        expected_cd,
        expected_offer=None,
        expected_stone=None,
        penalty=0,
    ) -> WorkAbortCleanupResult:
        operation_id, user_id, reason = str(operation_id).strip(), str(user_id), str(reason).strip()
        expected_cd = dict(expected_cd or {})
        expected_offer = dict(expected_offer) if expected_offer else None
        penalty = int(penalty)
        expected_stone = None if expected_stone is None else int(expected_stone)
        if not operation_id or reason not in {"active_abort", "offer_abort", "expired", "reset"} or penalty < 0:
            raise ValueError("invalid work cleanup request")
        if reason == "active_abort" and expected_stone is None:
            raise ValueError("active abort requires a stone snapshot")
        payload = _dump([user_id, reason, expected_cd, expected_offer, expected_stone, penalty])

        with self._lock, closing(db_backend.connect(self._database)) as conn:
            try:
                conn.execute("BEGIN IMMEDIATE")
                conn.execute(
                    "CREATE TABLE IF NOT EXISTS work_offer_snapshots("
                    "user_id TEXT PRIMARY KEY,snapshot TEXT NOT NULL,updated_at TEXT NOT NULL)"
                )
                conn.execute(
                    "CREATE TABLE IF NOT EXISTS work_active_snapshots("
                    "user_id TEXT PRIMARY KEY,snapshot TEXT NOT NULL,updated_at TEXT NOT NULL)"
                )
                conn.execute(
                    "CREATE TABLE IF NOT EXISTS work_abort_cleanup_operations("
                    "operation_id TEXT PRIMARY KEY,payload TEXT NOT NULL,reason TEXT NOT NULL,"
                    "penalty INTEGER NOT NULL,stone_remaining INTEGER NOT NULL,"
                    "created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP)"
                )
                previous = conn.execute(
                    "SELECT payload,penalty,stone_remaining FROM work_abort_cleanup_operations "
                    "WHERE operation_id=%s", (operation_id,),
                ).fetchone()
                if previous is not None:
                    conn.rollback()
                    if str(previous[0]) != payload:
                        return WorkAbortCleanupResult("operation_conflict")
                    return WorkAbortCleanupResult("duplicate", int(previous[1]), int(previous[2]))

                cd = conn.execute(
                    "SELECT COALESCE(type,0),create_time,scheduled_time FROM user_cd WHERE user_id=%s",
                    (user_id,),
                ).fetchone()
                user = conn.execute(
                    "SELECT COALESCE(stone,0) FROM user_xiuxian WHERE user_id=%s", (user_id,)
                ).fetchone()
                if cd is None or user is None:
                    conn.rollback()
                    return WorkAbortCleanupResult("user_missing")
                actual_cd = {"type": int(cd[0]), "create_time": cd[1], "scheduled_time": cd[2]}
                normalized_cd = {
                    "type": int(expected_cd.get("type", 0)),
                    "create_time": expected_cd.get("create_time"),
                    "scheduled_time": expected_cd.get("scheduled_time"),
                }
                if actual_cd != normalized_cd:
                    conn.rollback()
                    return WorkAbortCleanupResult("state_changed")
                if reason == "active_abort" and actual_cd["type"] != 2:
                    conn.rollback()
                    return WorkAbortCleanupResult("state_changed")

                stored = conn.execute(
                    "SELECT snapshot FROM work_offer_snapshots WHERE user_id=%s", (user_id,)
                ).fetchone()
                stored_offer = json.loads(str(stored[0])) if stored else None
                if stored_offer is not None and stored_offer != expected_offer:
                    conn.rollback()
                    return WorkAbortCleanupResult("state_changed")

                stone = int(user[0])
                if expected_stone is not None and stone != expected_stone:
                    conn.rollback()
                    return WorkAbortCleanupResult("state_changed")
                applied_penalty = min(penalty, stone) if reason == "active_abort" else 0
                remaining = stone - applied_penalty
                if applied_penalty:
                    changed = conn.execute(
                        "UPDATE user_xiuxian SET stone=%s WHERE user_id=%s AND stone=%s",
                        (remaining, user_id, stone),
                    )
                    if changed.rowcount != 1:
                        conn.rollback()
                        return WorkAbortCleanupResult("state_changed")
                conn.execute(
                    "UPDATE user_cd SET type=0,create_time=0,scheduled_time=NULL WHERE user_id=%s", (user_id,)
                )
                conn.execute("DELETE FROM work_offer_snapshots WHERE user_id=%s", (user_id,))
                conn.execute("DELETE FROM work_active_snapshots WHERE user_id=%s", (user_id,))
                conn.execute(
                    "INSERT INTO work_abort_cleanup_operations(operation_id,payload,reason,penalty,stone_remaining) "
                    "VALUES(%s,%s,%s,%s,%s)",
                    (operation_id, payload, reason, applied_penalty, remaining),
                )
                conn.commit()
                return WorkAbortCleanupResult("applied", applied_penalty, remaining)
            except Exception:
                conn.rollback()
                raise


__all__ = ["WorkAbortCleanupResult", "WorkAbortCleanupService"]
