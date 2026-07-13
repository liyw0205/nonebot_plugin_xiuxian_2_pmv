from __future__ import annotations

import json
import time
from contextlib import closing
from dataclasses import dataclass
from pathlib import Path
from threading import RLock

from ..xiuxian_utils import db_backend


@dataclass(frozen=True)
class BatchPetEggUseResult:
    status: str
    user_id: str
    item_id: int
    quantity: int
    pets: tuple[tuple[dict, str], ...]

    @property
    def succeeded(self) -> bool:
        return self.status in {"applied", "duplicate"}


class BatchItemUseService:
    """Consume a pet-egg batch and persist all pre-rolled pets atomically."""

    def __init__(
        self,
        game_database: str | Path,
        player_database: str | Path,
        lock: RLock | None = None,
    ) -> None:
        self._game_database = Path(game_database)
        self._player_database = Path(player_database)
        self._lock = lock or RLock()

    @staticmethod
    def _decode_pets(payload: str) -> tuple[tuple[dict, str], ...]:
        return tuple((dict(row[0]), str(row[1])) for row in json.loads(payload))

    @staticmethod
    def _current_pet_snapshot(conn, user_id: str) -> tuple[str, tuple[str, ...]] | None:
        meta = conn.execute(
            "SELECT active_uid FROM player_data.player_pet WHERE user_id=%s",
            (user_id,),
        ).fetchone()
        if meta is None:
            return None
        rows = conn.execute(
            "SELECT uid FROM player_data.player_pet_item WHERE user_id=%s ORDER BY uid",
            (user_id,),
        ).fetchall()
        return str(meta[0] or ""), tuple(str(row[0]) for row in rows)

    def use_pet_eggs(
        self,
        operation_id,
        user_id,
        item_id,
        quantity,
        expected_active_uid,
        expected_pet_uids,
        pets,
        *,
        bag_limit,
    ) -> BatchPetEggUseResult:
        operation_id = str(operation_id).strip()
        user_id = str(user_id)
        item_id = int(item_id)
        quantity = int(quantity)
        bag_limit = int(bag_limit)
        expected_active_uid = str(expected_active_uid or "")
        expected_pet_uids = tuple(sorted(str(uid) for uid in expected_pet_uids))
        normalized_pets = tuple((dict(pet), str(location)) for pet, location in pets)
        if not operation_id:
            raise ValueError("operation_id must not be empty")
        if quantity <= 0 or bag_limit <= 0 or len(normalized_pets) != quantity:
            raise ValueError("quantity, bag_limit and pet batch must be valid")
        if any(location not in {"active", "bag"} for _, location in normalized_pets):
            raise ValueError("pet location must be active or bag")
        pet_uids = [str(pet.get("uid", "")).strip() for pet, _ in normalized_pets]
        if any(not uid for uid in pet_uids) or len(set(pet_uids)) != len(pet_uids):
            raise ValueError("pet uids must be non-empty and unique")

        def result(status: str, result_pets=normalized_pets) -> BatchPetEggUseResult:
            return BatchPetEggUseResult(
                status, user_id, item_id, quantity, tuple(result_pets)
            )

        with self._lock, closing(db_backend.connect(self._game_database)) as conn:
            attached = False
            try:
                conn.execute("ATTACH DATABASE %s AS player_data", (str(self._player_database),))
                attached = True
                conn.execute("BEGIN IMMEDIATE")
                conn.execute(
                    """
                    CREATE TABLE IF NOT EXISTS batch_pet_egg_use_operations (
                        operation_id TEXT PRIMARY KEY,
                        user_id TEXT NOT NULL,
                        item_id INTEGER NOT NULL,
                        quantity INTEGER NOT NULL,
                        pets_json TEXT NOT NULL,
                        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                    )
                    """
                )
                previous = conn.execute(
                    "SELECT user_id, item_id, quantity, pets_json "
                    "FROM batch_pet_egg_use_operations WHERE operation_id=%s",
                    (operation_id,),
                ).fetchone()
                if previous is not None:
                    conn.rollback()
                    if (
                        str(previous[0]) != user_id
                        or int(previous[1]) != item_id
                        or int(previous[2]) != quantity
                    ):
                        return result("operation_conflict", ())
                    return result("duplicate", self._decode_pets(previous[3]))

                item = conn.execute(
                    "SELECT goods_num FROM back WHERE user_id=%s AND goods_id=%s",
                    (user_id, item_id),
                ).fetchone()
                if item is None or int(item[0] or 0) < quantity:
                    conn.rollback()
                    return result("item_insufficient", ())

                current_snapshot = self._current_pet_snapshot(conn, user_id)
                if current_snapshot is None:
                    conn.rollback()
                    return result("pet_state_missing", ())
                if current_snapshot != (expected_active_uid, expected_pet_uids):
                    conn.rollback()
                    return result("state_changed", ())
                if len(expected_pet_uids) + quantity > bag_limit:
                    conn.rollback()
                    return result("inventory_full", ())

                columns = set(conn.column_names("back"))
                bind_update = ""
                params: list[object] = [quantity]
                if "bind_num" in columns:
                    bind_update = ", bind_num=MIN(COALESCE(bind_num, 0), goods_num-%s)"
                    params.append(quantity)
                consumed = conn.execute(
                    f"UPDATE back SET goods_num=goods_num-%s{bind_update} "
                    "WHERE user_id=%s AND goods_id=%s AND goods_num>=%s",
                    (*params, user_id, item_id, quantity),
                )
                if consumed.rowcount != 1:
                    conn.rollback()
                    return result("state_changed", ())

                now = int(time.time())
                active_uid = expected_active_uid
                for pet, location in normalized_pets:
                    uid = str(pet["uid"])
                    is_active = location == "active"
                    if is_active:
                        active_uid = uid
                    skill = pet.get("skill") or ((pet.get("skills") or [{}])[0])
                    conn.execute(
                        "INSERT INTO player_data.player_pet_item "
                        "(id,user_id,uid,is_active,pet_id,stars,exp,total_exp,skill_id,created_at,updated_at) "
                        "VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)",
                        (
                            f"{user_id}:{uid}", user_id, uid, int(is_active),
                            str(pet.get("pet_id", "")), int(pet.get("stars", 1)),
                            int(pet.get("exp", 0)), int(pet.get("total_exp", 0)),
                            str(skill.get("skill_id", "") or "") or None, now, now,
                        ),
                    )
                conn.execute(
                    "UPDATE player_data.player_pet SET active_uid=%s WHERE user_id=%s",
                    (active_uid, user_id),
                )
                pets_json = json.dumps(normalized_pets, ensure_ascii=False)
                conn.execute(
                    "INSERT INTO batch_pet_egg_use_operations "
                    "(operation_id,user_id,item_id,quantity,pets_json) VALUES (%s,%s,%s,%s,%s)",
                    (operation_id, user_id, item_id, quantity, pets_json),
                )
                conn.commit()
                return result("applied")
            except Exception:
                conn.rollback()
                raise
            finally:
                if attached:
                    conn.execute("DETACH DATABASE player_data")


__all__ = ["BatchItemUseService", "BatchPetEggUseResult"]
