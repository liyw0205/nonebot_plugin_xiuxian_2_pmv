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
class WorkRefreshResult:
    status: str
    remaining_count: int = 0
    offer: dict | None = None

    @property
    def succeeded(self) -> bool:
        return self.status in {"applied", "duplicate"}


class WorkRefreshSettlementService:
    """Replace a work offer and consume one refresh count in one transaction."""

    def __init__(self, database: str | Path, lock: RLock | None = None) -> None:
        self._database = Path(database)
        self._lock = lock or RLock()

    def refresh(
        self,
        operation_id,
        user_id,
        expected_count,
        expected_cd,
        expected_offer,
        new_offer,
        force=False,
    ) -> WorkRefreshResult:
        operation_id, user_id = str(operation_id).strip(), str(user_id)
        expected_count = int(expected_count)
        expected_cd = dict(expected_cd or {})
        expected_offer = dict(expected_offer) if expected_offer else None
        new_offer = dict(new_offer)
        force = bool(force)
        if not operation_id or expected_count <= 0 or not new_offer.get("tasks"):
            raise ValueError("valid operation, refresh count and fixed offer are required")
        payload = _dump([user_id, expected_count, expected_cd, expected_offer, new_offer, force])
        offer_json = _dump(new_offer)

        with self._lock, closing(db_backend.connect(self._database)) as conn:
            try:
                conn.execute("BEGIN IMMEDIATE")
                conn.execute(
                    "CREATE TABLE IF NOT EXISTS work_offer_snapshots("
                    "user_id TEXT PRIMARY KEY,snapshot TEXT NOT NULL,updated_at TEXT NOT NULL)"
                )
                conn.execute(
                    "CREATE TABLE IF NOT EXISTS work_refresh_operations("
                    "operation_id TEXT PRIMARY KEY,payload TEXT NOT NULL,remaining_count INTEGER NOT NULL,"
                    "offer_snapshot TEXT NOT NULL,created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP)"
                )
                previous = conn.execute(
                    "SELECT payload,remaining_count,offer_snapshot FROM work_refresh_operations "
                    "WHERE operation_id=%s", (operation_id,),
                ).fetchone()
                if previous is not None:
                    conn.rollback()
                    if str(previous[0]) != payload:
                        return WorkRefreshResult("operation_conflict")
                    return WorkRefreshResult("duplicate", int(previous[1]), json.loads(str(previous[2])))

                user = conn.execute(
                    "SELECT COALESCE(work_num,0) FROM user_xiuxian WHERE user_id=%s", (user_id,)
                ).fetchone()
                cd = conn.execute(
                    "SELECT COALESCE(type,0),create_time,scheduled_time FROM user_cd WHERE user_id=%s",
                    (user_id,),
                ).fetchone()
                if user is None or cd is None:
                    conn.rollback()
                    return WorkRefreshResult("user_missing")
                actual_cd = {"type": int(cd[0]), "create_time": cd[1], "scheduled_time": cd[2]}
                normalized_cd = {
                    "type": int(expected_cd.get("type", 0)),
                    "create_time": expected_cd.get("create_time"),
                    "scheduled_time": expected_cd.get("scheduled_time"),
                }
                if int(user[0]) != expected_count or actual_cd != normalized_cd or actual_cd["type"] != 0:
                    conn.rollback()
                    return WorkRefreshResult("state_changed")

                stored = conn.execute(
                    "SELECT snapshot FROM work_offer_snapshots WHERE user_id=%s", (user_id,)
                ).fetchone()
                stored_offer = json.loads(str(stored[0])) if stored else None
                if stored_offer is not None and stored_offer != expected_offer:
                    conn.rollback()
                    return WorkRefreshResult("state_changed")
                if not force and expected_offer and int(expected_offer.get("status", 1)) == 1:
                    conn.rollback()
                    return WorkRefreshResult("offer_exists")

                remaining = expected_count - 1
                changed = conn.execute(
                    "UPDATE user_xiuxian SET work_num=%s WHERE user_id=%s AND work_num=%s",
                    (remaining, user_id, expected_count),
                )
                if changed.rowcount != 1:
                    conn.rollback()
                    return WorkRefreshResult("state_changed")
                conn.execute(
                    "INSERT INTO work_offer_snapshots(user_id,snapshot,updated_at) VALUES(%s,%s,%s) "
                    "ON CONFLICT(user_id) DO UPDATE SET snapshot=EXCLUDED.snapshot,updated_at=EXCLUDED.updated_at",
                    (user_id, offer_json, str(new_offer.get("refresh_time", ""))),
                )
                conn.execute(
                    "INSERT INTO work_refresh_operations(operation_id,payload,remaining_count,offer_snapshot) "
                    "VALUES(%s,%s,%s,%s)", (operation_id, payload, remaining, offer_json),
                )
                conn.commit()
                return WorkRefreshResult("applied", remaining, new_offer)
            except Exception:
                conn.rollback()
                raise


__all__ = ["WorkRefreshResult", "WorkRefreshSettlementService"]
