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
    cost: int = 0
    pets: tuple = ()
    updated_meta: tuple = ()
    bag_limit: int = 0

    @property
    def succeeded(self) -> bool:
        return self.status in {"applied", "duplicate"}


class PetHatchService:
    """Charge stones and persist a pre-rolled hatch batch in one cross-database transaction."""

    def __init__(self, game_database: str | Path, player_database: str | Path, lock: RLock | None = None) -> None:
        self._game_database = Path(game_database)
        self._player_database = Path(player_database)
        self._lock = lock or RLock()

    @staticmethod
    def _payload(user_id, cost, count) -> str:
        # Request identity only — stone/meta/pets are outcomes or concurrency checks.
        return json.dumps([str(user_id), int(cost), int(count)], ensure_ascii=True, separators=(",", ":"))

    @staticmethod
    def _result_from_row(status: str, payload: str, result_json: str | None) -> PetHatchResult:
        cost = 0
        count = 0
        try:
            body = json.loads(payload or "[]")
            if isinstance(body, list) and len(body) >= 3:
                cost = int(body[1] or 0)
                count = int(body[2] or 0)
        except Exception:
            pass
        pets: tuple = ()
        updated_meta: tuple = ()
        bag_limit = 0
        if result_json:
            try:
                data = json.loads(result_json)
                pets = tuple((dict(pet), bool(active)) for pet, active in (data.get("pets") or []))
                updated_meta = tuple(data.get("updated_meta") or ())
                bag_limit = int(data.get("bag_limit") or 0)
            except Exception:
                pets, updated_meta, bag_limit = (), (), 0
        if not pets and count:
            # keep cost/count recoverable even if result blob missing
            pass
        return PetHatchResult(status, cost=cost, pets=pets, updated_meta=updated_meta, bag_limit=bag_limit)

    def get_result(self, operation_id: str) -> PetHatchResult | None:
        operation_id = str(operation_id).strip()
        if not operation_id:
            return None
        with self._lock, closing(db_backend.connect(self._game_database)) as conn:
            conn.execute(
                "CREATE TABLE IF NOT EXISTS pet_hatch_operations("
                "operation_id TEXT PRIMARY KEY,payload TEXT NOT NULL,"
                "result_json TEXT,created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP)"
            )
            # migrate older schema without result_json
            cols = {str(r[1]) for r in conn.execute("PRAGMA table_info(pet_hatch_operations)").fetchall()}
            if "result_json" not in cols:
                try:
                    conn.execute("ALTER TABLE pet_hatch_operations ADD COLUMN result_json TEXT")
                    conn.commit()
                except Exception:
                    pass
            row = conn.execute(
                "SELECT payload,result_json FROM pet_hatch_operations WHERE operation_id=%s",
                (operation_id,),
            ).fetchone()
            if row is None:
                return None
            return self._result_from_row("duplicate", str(row[0] or ""), row[1])

    def hatch(self, operation_id, user_id, expected_stone, cost, expected_meta, pets, updated_meta, bag_limit) -> PetHatchResult:
        operation_id, user_id = str(operation_id).strip(), str(user_id)
        expected_stone, cost, bag_limit = int(expected_stone), int(cost), int(bag_limit)
        normalized = tuple((dict(pet), bool(active)) for pet, active in pets)
        count = len(normalized)
        payload = self._payload(user_id, cost, count)
        result_json = json.dumps(
            {
                "pets": [[pet, bool(active)] for pet, active in normalized],
                "updated_meta": list(updated_meta),
                "bag_limit": bag_limit,
            },
            ensure_ascii=True,
            separators=(",", ":"),
        )
        if not operation_id or cost < 0 or not normalized:
            raise ValueError("valid operation and hatch batch are required")
        with self._lock, closing(db_backend.connect(self._game_database)) as conn:
            attached = False
            try:
                conn.execute("ATTACH DATABASE %s AS player_data", (str(self._player_database),))
                attached = True
                conn.execute("BEGIN IMMEDIATE")
                conn.execute(
                    "CREATE TABLE IF NOT EXISTS pet_hatch_operations("
                    "operation_id TEXT PRIMARY KEY,payload TEXT NOT NULL,"
                    "result_json TEXT,created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP)"
                )
                cols = {str(r[1]) for r in conn.execute("PRAGMA table_info(pet_hatch_operations)").fetchall()}
                if "result_json" not in cols:
                    conn.execute("ALTER TABLE pet_hatch_operations ADD COLUMN result_json TEXT")
                old = conn.execute(
                    "SELECT payload,result_json FROM pet_hatch_operations WHERE operation_id=%s",
                    (operation_id,),
                ).fetchone()
                if old is not None:
                    conn.rollback()
                    if str(old[0]) != payload:
                        return PetHatchResult("state_changed")
                    return self._result_from_row("duplicate", str(old[0] or ""), old[1])
                user = conn.execute("SELECT stone FROM user_xiuxian WHERE user_id=%s", (user_id,)).fetchone()
                if user is None:
                    conn.rollback()
                    return PetHatchResult("user_missing")
                if int(user[0]) != expected_stone:
                    conn.rollback()
                    return PetHatchResult("state_changed")
                if expected_stone < cost:
                    conn.rollback()
                    return PetHatchResult("stone_missing")
                meta = conn.execute(
                    "SELECT active_uid,egg_pity_count,egg_pity_no_mythic_count,travel "
                    "FROM player_data.player_pet WHERE user_id=%s",
                    (user_id,),
                ).fetchone()
                current_meta = None if meta is None else [meta[0] or "", int(meta[1] or 0), int(meta[2] or 0), meta[3]]
                if current_meta != list(expected_meta):
                    conn.rollback()
                    return PetHatchResult("state_changed")
                owned = int(
                    conn.execute(
                        "SELECT COUNT(*) FROM player_data.player_pet_item WHERE user_id=%s",
                        (user_id,),
                    ).fetchone()[0]
                )
                if owned + len(normalized) > bag_limit:
                    conn.rollback()
                    return PetHatchResult("inventory_full")
                now = int(time.time())
                for pet, active in normalized:
                    skill = pet.get("skill") or ((pet.get("skills") or [{}])[0])
                    conn.execute(
                        "INSERT INTO player_data.player_pet_item("
                        "id,user_id,uid,is_active,pet_id,stars,exp,total_exp,skill_id,created_at,updated_at) "
                        "VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)",
                        (
                            f"{user_id}:{pet['uid']}",
                            user_id,
                            str(pet["uid"]),
                            int(active),
                            str(pet.get("pet_id", "")),
                            int(pet.get("stars", 1)),
                            int(pet.get("exp", 0)),
                            int(pet.get("total_exp", 0)),
                            str(skill.get("skill_id", "")) or None,
                            now,
                            now,
                        ),
                    )
                conn.execute(
                    "UPDATE user_xiuxian SET stone=stone-%s WHERE user_id=%s AND stone=%s",
                    (cost, user_id, expected_stone),
                )
                conn.execute(
                    "UPDATE player_data.player_pet SET active_uid=%s,egg_pity_count=%s,egg_pity_no_mythic_count=%s WHERE user_id=%s",
                    (*updated_meta, user_id),
                )
                conn.execute(
                    "INSERT INTO pet_hatch_operations(operation_id,payload,result_json) VALUES (%s,%s,%s)",
                    (operation_id, payload, result_json),
                )
                conn.commit()
                return PetHatchResult(
                    "applied",
                    cost=cost,
                    pets=normalized,
                    updated_meta=tuple(updated_meta),
                    bag_limit=bag_limit,
                )
            except Exception:
                conn.rollback()
                raise
            finally:
                if attached:
                    conn.execute("DETACH DATABASE player_data")


__all__ = ["PetHatchResult", "PetHatchService"]
