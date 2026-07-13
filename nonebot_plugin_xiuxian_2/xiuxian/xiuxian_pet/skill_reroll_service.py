from __future__ import annotations

import json
import time
from contextlib import closing
from dataclasses import dataclass
from pathlib import Path
from threading import RLock

from ..xiuxian_utils import db_backend


@dataclass(frozen=True)
class PetSkillRerollResult:
    status: str
    skill_id: str = ""

    @property
    def succeeded(self) -> bool:
        return self.status in {"applied", "duplicate"}


class PetSkillRerollService:
    """Apply a pre-rolled skill and consume one item in a cross-database transaction."""

    def __init__(self, game_database: str | Path, player_database: str | Path, lock: RLock | None = None) -> None:
        self._game_database = Path(game_database)
        self._player_database = Path(player_database)
        self._lock = lock or RLock()

    def reroll(self, operation_id, user_id, expected_pet, new_skill_id, item_id) -> PetSkillRerollResult:
        operation_id = str(operation_id).strip()
        user_id, new_skill_id = str(user_id), str(new_skill_id)
        expected_pet = tuple(expected_pet)
        item_id = int(item_id)
        if not operation_id or len(expected_pet) != 7 or not new_skill_id:
            raise ValueError("operation, pet snapshot and new skill are required")
        payload = json.dumps(
            [user_id, expected_pet, new_skill_id, item_id],
            ensure_ascii=True,
            separators=(",", ":"),
        )

        with self._lock, closing(db_backend.connect(self._game_database)) as conn:
            attached = False
            try:
                conn.execute("ATTACH DATABASE %s AS player_data", (str(self._player_database),))
                attached = True
                conn.execute("BEGIN IMMEDIATE")
                conn.execute(
                    "CREATE TABLE IF NOT EXISTS pet_skill_reroll_operations ("
                    "operation_id TEXT PRIMARY KEY,payload TEXT NOT NULL,skill_id TEXT NOT NULL,"
                    "created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP)"
                )
                old = conn.execute(
                    "SELECT payload,skill_id FROM pet_skill_reroll_operations WHERE operation_id=%s",
                    (operation_id,),
                ).fetchone()
                if old is not None:
                    conn.rollback()
                    if str(old[0]) != payload:
                        return PetSkillRerollResult("state_changed")
                    return PetSkillRerollResult("duplicate", str(old[1]))

                pet = conn.execute(
                    "SELECT uid,pet_id,stars,exp,total_exp,COALESCE(skill_id,''),is_active "
                    "FROM player_data.player_pet_item WHERE user_id=%s AND uid=%s",
                    (user_id, str(expected_pet[0])),
                ).fetchone()
                if pet is None or tuple(pet) != expected_pet:
                    conn.rollback()
                    return PetSkillRerollResult("state_changed")
                item = conn.execute(
                    "SELECT goods_num FROM back WHERE user_id=%s AND goods_id=%s",
                    (user_id, item_id),
                ).fetchone()
                if item is None or int(item[0]) < 1:
                    conn.rollback()
                    return PetSkillRerollResult("item_missing")

                consumed = conn.execute(
                    "UPDATE back SET goods_num=goods_num-1 WHERE user_id=%s AND goods_id=%s AND goods_num>=1",
                    (user_id, item_id),
                ).rowcount
                updated = conn.execute(
                    "UPDATE player_data.player_pet_item SET skill_id=%s,updated_at=%s "
                    "WHERE user_id=%s AND uid=%s AND COALESCE(skill_id,'')=%s",
                    (new_skill_id, int(time.time()), user_id, str(expected_pet[0]), str(expected_pet[5])),
                ).rowcount
                if consumed != 1 or updated != 1:
                    conn.rollback()
                    return PetSkillRerollResult("state_changed")
                conn.execute(
                    "INSERT INTO pet_skill_reroll_operations(operation_id,payload,skill_id) VALUES (%s,%s,%s)",
                    (operation_id, payload, new_skill_id),
                )
                conn.commit()
                return PetSkillRerollResult("applied", new_skill_id)
            except Exception:
                conn.rollback()
                raise
            finally:
                if attached:
                    conn.execute("DETACH DATABASE player_data")


__all__ = ["PetSkillRerollResult", "PetSkillRerollService"]
