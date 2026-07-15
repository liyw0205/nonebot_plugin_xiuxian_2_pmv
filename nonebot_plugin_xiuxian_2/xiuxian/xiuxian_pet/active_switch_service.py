from __future__ import annotations

import json
import time
from contextlib import closing
from dataclasses import dataclass
from pathlib import Path
from threading import RLock

from ..xiuxian_utils import db_backend


@dataclass(frozen=True)
class PetActiveSwitchResult:
    status: str
    active_uid: str = ""

    @property
    def succeeded(self) -> bool:
        return self.status in {"applied", "duplicate", "already_active"}


class PetActiveSwitchService:
    def __init__(self, player_database: str | Path, lock: RLock | None = None) -> None:
        self._database = Path(player_database)
        self._lock = lock or RLock()

    def switch(self, operation_id, user_id, expected_active_uid, target_uid, travel_pet_uid=""):
        operation_id = str(operation_id).strip()
        user_id = str(user_id)
        expected_active_uid = str(expected_active_uid or "")
        target_uid = str(target_uid).strip()
        travel_pet_uid = str(travel_pet_uid or "")
        if not operation_id or not user_id or not target_uid:
            raise ValueError("operation, user and target pet are required")
        # Request identity only — expected_active_uid / travel_pet_uid are concurrency checks.
        payload = json.dumps(
            [user_id, target_uid],
            ensure_ascii=True,
            separators=(",", ":"),
        )
        with self._lock, closing(db_backend.connect(self._database)) as conn:
            try:
                conn.execute("BEGIN IMMEDIATE")
                conn.execute(
                    "CREATE TABLE IF NOT EXISTS pet_active_switch_operations("
                    "operation_id TEXT PRIMARY KEY,payload TEXT NOT NULL,active_uid TEXT NOT NULL,"
                    "created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP)"
                )
                previous = conn.execute(
                    "SELECT payload,active_uid FROM pet_active_switch_operations WHERE operation_id=%s",
                    (operation_id,),
                ).fetchone()
                if previous is not None:
                    conn.rollback()
                    if str(previous[0]) != payload:
                        return PetActiveSwitchResult("operation_conflict")
                    return PetActiveSwitchResult("duplicate", str(previous[1]))

                target = conn.execute(
                    "SELECT COALESCE(is_active,0) FROM player_pet_item WHERE user_id=%s AND uid=%s",
                    (user_id, target_uid),
                ).fetchone()
                if target is None:
                    conn.rollback()
                    return PetActiveSwitchResult("pet_missing")
                current = conn.execute(
                    "SELECT uid FROM player_pet_item WHERE user_id=%s AND COALESCE(is_active,0)=1",
                    (user_id,),
                ).fetchall()
                current_uids = tuple(str(row[0]) for row in current)
                actual_active = current_uids[0] if len(current_uids) == 1 else ""
                if len(current_uids) > 1 or actual_active != expected_active_uid:
                    conn.rollback()
                    return PetActiveSwitchResult("state_changed")
                if target_uid == actual_active:
                    conn.rollback()
                    return PetActiveSwitchResult("already_active", target_uid)
                if target_uid == travel_pet_uid:
                    conn.rollback()
                    return PetActiveSwitchResult("pet_traveling")
                meta = conn.execute("SELECT active_uid FROM player_pet WHERE user_id=%s", (user_id,)).fetchone()
                if meta is None or str(meta[0] or "") != expected_active_uid:
                    conn.rollback()
                    return PetActiveSwitchResult("state_changed")

                now = int(time.time())
                conn.execute(
                    "UPDATE player_pet_item SET is_active=0,updated_at=%s WHERE user_id=%s AND COALESCE(is_active,0)=1",
                    (now, user_id),
                )
                changed = conn.execute(
                    "UPDATE player_pet_item SET is_active=1,updated_at=%s WHERE user_id=%s AND uid=%s AND COALESCE(is_active,0)=0",
                    (now, user_id, target_uid),
                )
                if changed.rowcount != 1:
                    raise RuntimeError("target pet state changed")
                changed = conn.execute(
                    "UPDATE player_pet SET active_uid=%s,active=%s WHERE user_id=%s AND COALESCE(active_uid,'')=%s",
                    (target_uid, target_uid, user_id, expected_active_uid),
                )
                if changed.rowcount != 1:
                    raise RuntimeError("pet metadata state changed")
                conn.execute(
                    "INSERT INTO pet_active_switch_operations(operation_id,payload,active_uid) VALUES(%s,%s,%s)",
                    (operation_id, payload, target_uid),
                )
                conn.commit()
                return PetActiveSwitchResult("applied", target_uid)
            except Exception:
                conn.rollback()
                raise


__all__ = ["PetActiveSwitchResult", "PetActiveSwitchService"]
