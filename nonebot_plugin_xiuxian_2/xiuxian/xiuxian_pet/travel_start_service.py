from __future__ import annotations

import json
from contextlib import closing
from dataclasses import dataclass
from pathlib import Path
from threading import RLock

from ..xiuxian_utils import db_backend


@dataclass(frozen=True)
class PetTravelStartResult:
    status: str

    @property
    def succeeded(self) -> bool:
        return self.status in {"applied", "duplicate"}


class PetTravelStartService:
    """Persist a travel snapshot only while the selected pet and meta snapshot still match."""

    def __init__(self, player_database: str | Path, lock: RLock | None = None) -> None:
        self._database = Path(player_database)
        self._lock = lock or RLock()

    def start(self, operation_id, user_id, pet_uid, expected_travel, travel) -> PetTravelStartResult:
        operation_id = str(operation_id).strip()
        user_id = str(user_id)
        pet_uid = str(pet_uid)
        expected_json = None if expected_travel is None else json.dumps(expected_travel, ensure_ascii=False, sort_keys=True)
        travel_json = json.dumps(travel, ensure_ascii=False, sort_keys=True)
        payload = json.dumps([user_id, pet_uid, expected_json, travel_json], ensure_ascii=True)
        if not operation_id or not user_id or not pet_uid or not isinstance(travel, dict):
            raise ValueError("operation, user, pet and travel are required")

        with self._lock, closing(db_backend.connect(self._database)) as conn:
            try:
                conn.execute("BEGIN IMMEDIATE")
                conn.execute(
                    "CREATE TABLE IF NOT EXISTS pet_travel_start_operations "
                    "(operation_id TEXT PRIMARY KEY,payload TEXT NOT NULL,created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP)"
                )
                previous = conn.execute(
                    "SELECT payload FROM pet_travel_start_operations WHERE operation_id=%s", (operation_id,)
                ).fetchone()
                if previous is not None:
                    conn.rollback()
                    return PetTravelStartResult("duplicate" if str(previous[0]) == payload else "state_changed")
                meta = conn.execute("SELECT travel FROM player_pet WHERE user_id=%s", (user_id,)).fetchone()
                current = None if meta is None or meta[0] is None else json.dumps(json.loads(str(meta[0])), ensure_ascii=False, sort_keys=True)
                if meta is None:
                    conn.rollback()
                    return PetTravelStartResult("user_missing")
                if current != expected_json:
                    conn.rollback()
                    return PetTravelStartResult("state_changed")
                pet = conn.execute(
                    "SELECT is_active FROM player_pet_item WHERE user_id=%s AND uid=%s", (user_id, pet_uid)
                ).fetchone()
                if pet is None or int(pet[0]) != 1:
                    conn.rollback()
                    return PetTravelStartResult("pet_changed")
                if conn.execute(
                    "UPDATE player_pet SET travel=%s WHERE user_id=%s AND travel IS NULL", (travel_json, user_id)
                ).rowcount != 1:
                    conn.rollback()
                    return PetTravelStartResult("state_changed")
                conn.execute("INSERT INTO pet_travel_start_operations VALUES (%s,%s,CURRENT_TIMESTAMP)", (operation_id, payload))
                conn.commit()
                return PetTravelStartResult("applied")
            except Exception:
                conn.rollback()
                raise


__all__ = ["PetTravelStartResult", "PetTravelStartService"]
