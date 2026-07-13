from __future__ import annotations

import json
from contextlib import closing
from dataclasses import dataclass
from pathlib import Path
from threading import RLock

from ..xiuxian_utils import db_backend


@dataclass(frozen=True)
class PetReleaseResult:
    status: str
    refund: int = 0
    released_uids: tuple[str, ...] = ()

    @property
    def succeeded(self) -> bool:
        return self.status in {"applied", "duplicate"}


class PetReleaseService:
    """Delete one or more pets and grant the combined refund atomically."""

    def __init__(self, game: str | Path, player: str | Path, lock: RLock | None = None) -> None:
        self._game = Path(game)
        self._player = Path(player)
        self._lock = lock or RLock()

    def release(self, operation_id, user_id, uid, expected_exp, refund_item, refund, max_goods) -> PetReleaseResult:
        return self.release_batch(
            operation_id,
            user_id,
            [{"uid": uid, "total_exp": expected_exp, "is_active": 1}],
            refund_item,
            "天地灵髓",
            "特殊道具",
            refund,
            max_goods,
            allow_active=True,
        )

    def release_batch(
        self,
        operation_id,
        user_id,
        expected_pets,
        refund_item,
        refund_name,
        refund_type,
        refund,
        max_goods,
        allow_active=False,
    ) -> PetReleaseResult:
        operation_id = str(operation_id).strip()
        user_id = str(user_id)
        refund_item, refund, max_goods = map(int, (refund_item, refund, max_goods))
        pets = tuple(
            sorted(
                (
                    str(pet["uid"]),
                    int(pet.get("total_exp", 0)),
                    int(pet.get("is_active", 0)),
                )
                for pet in expected_pets
            )
        )
        if not operation_id or not pets or len({pet[0] for pet in pets}) != len(pets):
            raise ValueError("operation and unique pet snapshots are required")
        if min(refund_item, refund, max_goods) < 0:
            raise ValueError("refund values must be non-negative")
        payload = json.dumps(
            [user_id, pets, refund_item, str(refund_name), str(refund_type), refund, max_goods, bool(allow_active)],
            ensure_ascii=True,
            separators=(",", ":"),
        )

        with self._lock, closing(db_backend.connect(self._game)) as conn:
            attached = False
            try:
                conn.execute("ATTACH DATABASE %s AS player_data", (str(self._player),))
                attached = True
                conn.execute("BEGIN IMMEDIATE")
                conn.execute(
                    "CREATE TABLE IF NOT EXISTS pet_release_operations ("
                    "operation_id TEXT PRIMARY KEY,payload TEXT NOT NULL,refund INTEGER NOT NULL,"
                    "released_uids TEXT NOT NULL,created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP)"
                )
                old = conn.execute(
                    "SELECT payload,refund,released_uids FROM pet_release_operations WHERE operation_id=%s",
                    (operation_id,),
                ).fetchone()
                if old is not None:
                    conn.rollback()
                    if str(old[0]) != payload:
                        return PetReleaseResult("state_changed")
                    return PetReleaseResult("duplicate", int(old[1]), tuple(json.loads(str(old[2]))))

                placeholders = ",".join(["%s"] * len(pets))
                uids = tuple(pet[0] for pet in pets)
                rows = conn.execute(
                    f"SELECT uid,COALESCE(total_exp,0),COALESCE(is_active,0) "
                    f"FROM player_data.player_pet_item WHERE user_id=%s AND uid IN ({placeholders})",
                    (user_id, *uids),
                ).fetchall()
                current = tuple(sorted((str(row[0]), int(row[1]), int(row[2])) for row in rows))
                if current != pets:
                    conn.rollback()
                    return PetReleaseResult("state_changed")
                if not allow_active and any(pet[2] for pet in pets):
                    conn.rollback()
                    return PetReleaseResult("active_pet")

                inventory = conn.execute(
                    "SELECT COALESCE(goods_num,0) FROM back WHERE user_id=%s AND goods_id=%s",
                    (user_id, refund_item),
                ).fetchone()
                if (int(inventory[0]) if inventory else 0) + refund > max_goods:
                    conn.rollback()
                    return PetReleaseResult("inventory_full")

                deleted = conn.execute(
                    f"DELETE FROM player_data.player_pet_item WHERE user_id=%s AND uid IN ({placeholders})",
                    (user_id, *uids),
                ).rowcount
                if deleted != len(pets):
                    conn.rollback()
                    return PetReleaseResult("state_changed")
                if allow_active and any(pet[2] for pet in pets):
                    conn.execute(
                        "UPDATE player_data.player_pet SET active_uid=NULL,active=NULL WHERE user_id=%s",
                        (user_id,),
                    )
                if refund:
                    conn.execute(
                        "INSERT INTO back(user_id,goods_id,goods_name,goods_type,goods_num) VALUES (%s,%s,%s,%s,%s) "
                        "ON CONFLICT(user_id,goods_id) DO UPDATE SET goods_name=EXCLUDED.goods_name,"
                        "goods_type=EXCLUDED.goods_type,goods_num=back.goods_num+EXCLUDED.goods_num",
                        (user_id, refund_item, str(refund_name), str(refund_type), refund),
                    )
                released_json = json.dumps(uids, ensure_ascii=True)
                conn.execute(
                    "INSERT INTO pet_release_operations(operation_id,payload,refund,released_uids) VALUES (%s,%s,%s,%s)",
                    (operation_id, payload, refund, released_json),
                )
                conn.commit()
                return PetReleaseResult("applied", refund, uids)
            except Exception:
                conn.rollback()
                raise
            finally:
                if attached:
                    conn.execute("DETACH DATABASE player_data")


__all__ = ["PetReleaseResult", "PetReleaseService"]
