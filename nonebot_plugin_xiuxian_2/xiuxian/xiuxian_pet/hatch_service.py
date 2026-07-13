from __future__ import annotations

import json
import time
from contextlib import closing
from dataclasses import dataclass
from pathlib import Path
from threading import RLock

from ..xiuxian_utils import db_backend


@dataclass(frozen=True)
class PetHatchResult:
    status: str

    @property
    def succeeded(self) -> bool:
        return self.status in {"applied", "duplicate"}


class PetHatchService:
    """Charge stones and persist a pre-rolled hatch batch in one cross-database transaction."""

    def __init__(self, game_database: str | Path, player_database: str | Path, lock: RLock | None = None) -> None:
        self._game_database = Path(game_database)
        self._player_database = Path(player_database)
        self._lock = lock or RLock()

    def hatch(self, operation_id, user_id, expected_stone, cost, expected_meta, pets, updated_meta, bag_limit) -> PetHatchResult:
        operation_id, user_id = str(operation_id).strip(), str(user_id)
        expected_stone, cost, bag_limit = int(expected_stone), int(cost), int(bag_limit)
        normalized = tuple((dict(pet), bool(active)) for pet, active in pets)
        payload = json.dumps([user_id, expected_stone, cost, expected_meta, normalized, updated_meta, bag_limit], ensure_ascii=True, sort_keys=True)
        if not operation_id or cost < 0 or not normalized:
            raise ValueError("valid operation and hatch batch are required")
        with self._lock, closing(db_backend.connect(self._game_database)) as conn:
            attached = False
            try:
                conn.execute("ATTACH DATABASE %s AS player_data", (str(self._player_database),))
                attached = True
                conn.execute("BEGIN IMMEDIATE")
                conn.execute("CREATE TABLE IF NOT EXISTS pet_hatch_operations(operation_id TEXT PRIMARY KEY,payload TEXT NOT NULL,created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP)")
                old = conn.execute("SELECT payload FROM pet_hatch_operations WHERE operation_id=%s", (operation_id,)).fetchone()
                if old is not None:
                    conn.rollback(); return PetHatchResult("duplicate" if str(old[0]) == payload else "state_changed")
                user = conn.execute("SELECT stone FROM user_xiuxian WHERE user_id=%s", (user_id,)).fetchone()
                if user is None: conn.rollback(); return PetHatchResult("user_missing")
                if int(user[0]) != expected_stone: conn.rollback(); return PetHatchResult("state_changed")
                if expected_stone < cost: conn.rollback(); return PetHatchResult("stone_missing")
                meta = conn.execute("SELECT active_uid,egg_pity_count,egg_pity_no_mythic_count,travel FROM player_data.player_pet WHERE user_id=%s", (user_id,)).fetchone()
                current_meta = None if meta is None else [meta[0] or "", int(meta[1] or 0), int(meta[2] or 0), meta[3]]
                if current_meta != list(expected_meta): conn.rollback(); return PetHatchResult("state_changed")
                owned = int(conn.execute("SELECT COUNT(*) FROM player_data.player_pet_item WHERE user_id=%s", (user_id,)).fetchone()[0])
                if owned + len(normalized) > bag_limit: conn.rollback(); return PetHatchResult("inventory_full")
                now = int(time.time())
                for pet, active in normalized:
                    skill = pet.get("skill") or ((pet.get("skills") or [{}])[0])
                    conn.execute(
                        "INSERT INTO player_data.player_pet_item(id,user_id,uid,is_active,pet_id,stars,exp,total_exp,skill_id,created_at,updated_at) VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)",
                        (f"{user_id}:{pet['uid']}", user_id, str(pet["uid"]), int(active), str(pet.get("pet_id", "")), int(pet.get("stars", 1)), int(pet.get("exp", 0)), int(pet.get("total_exp", 0)), str(skill.get("skill_id", "")) or None, now, now),
                    )
                conn.execute("UPDATE user_xiuxian SET stone=stone-%s WHERE user_id=%s AND stone=%s", (cost, user_id, expected_stone))
                conn.execute("UPDATE player_data.player_pet SET active_uid=%s,egg_pity_count=%s,egg_pity_no_mythic_count=%s WHERE user_id=%s", (*updated_meta, user_id))
                conn.execute("INSERT INTO pet_hatch_operations VALUES (%s,%s,CURRENT_TIMESTAMP)", (operation_id, payload))
                conn.commit(); return PetHatchResult("applied")
            except Exception:
                conn.rollback(); raise
            finally:
                if attached: conn.execute("DETACH DATABASE player_data")


__all__ = ["PetHatchResult", "PetHatchService"]
