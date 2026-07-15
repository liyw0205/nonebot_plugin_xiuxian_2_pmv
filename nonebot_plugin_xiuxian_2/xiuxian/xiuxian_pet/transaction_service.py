from __future__ import annotations

import json
from contextlib import closing
from dataclasses import dataclass, field
from pathlib import Path
from threading import RLock
from datetime import datetime
from ..xiuxian_utils import db_backend
import time

@dataclass(frozen=True)
class PetTravelClaimResult:
    status: str
    stone: int
    exp: int
    items: tuple[tuple[int, int], ...]

class PetTravelClaimService:
    """Consume a completed travel and grant all rewards atomically."""

    def __init__(self, game_database: str | Path, player_database: str | Path, lock: RLock | None = None) -> None:
        self._game_database = Path(game_database)
        self._player_database = Path(player_database)
        self._lock = lock or RLock()

    def claim(self, operation_id, user_id, expected_travel, stone, exp, items, max_goods_num) -> PetTravelClaimResult:
        operation_id = str(operation_id).strip()
        user_id = str(user_id)
        stone = int(stone)
        exp = int(exp)
        max_goods_num = int(max_goods_num)
        rewards = tuple(
            (int(item["id"]), str(item["name"]), str(item["type"]), int(item["amount"]))
            for item in items
            if int(item.get("id", 0)) > 0 and int(item.get("amount", 0)) > 0
        )
        if not operation_id or not isinstance(expected_travel, dict) or min(stone, exp, max_goods_num) < 0:
            raise ValueError("valid operation, travel and rewards are required")
        travel_json = json.dumps(expected_travel, ensure_ascii=False, sort_keys=True, separators=(",", ":"))
        payload = json.dumps([user_id, expected_travel, stone, exp, rewards, max_goods_num], ensure_ascii=True, sort_keys=True)

        def result(status: str) -> PetTravelClaimResult:
            granted = status in {"applied", "duplicate"}
            return PetTravelClaimResult(status, stone if granted else 0, exp if granted else 0, tuple((row[0], row[3]) for row in rewards) if granted else ())

        with self._lock, closing(db_backend.connect(self._game_database)) as conn:
            attached = False
            try:
                conn.execute("ATTACH DATABASE %s AS player_data", (str(self._player_database),))
                attached = True
                conn.execute("BEGIN IMMEDIATE")
                conn.execute(
                    "CREATE TABLE IF NOT EXISTS pet_travel_claim_operations ("
                    "operation_id TEXT PRIMARY KEY, payload TEXT NOT NULL, created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP)"
                )
                previous = conn.execute(
                    "SELECT payload FROM pet_travel_claim_operations WHERE operation_id=%s", (operation_id,)
                ).fetchone()
                if previous is not None:
                    conn.rollback()
                    return result("duplicate" if str(previous[0]) == payload else "state_changed")
                if conn.execute("SELECT 1 FROM user_xiuxian WHERE user_id=%s", (user_id,)).fetchone() is None:
                    conn.rollback()
                    return result("user_missing")
                table = conn.execute(
                    "SELECT 1 FROM player_data.sqlite_master WHERE type='table' AND name=%s", ("player_pet",)
                ).fetchone()
                columns = (
                    {str(row[1]) for row in conn.execute("PRAGMA player_data.table_info(player_pet)").fetchall()}
                    if table is not None else set()
                )
                if "travel" not in columns:
                    conn.rollback()
                    return result("state_changed")
                row = conn.execute("SELECT travel FROM player_data.player_pet WHERE user_id=%s", (user_id,)).fetchone()
                try:
                    current_travel = json.loads(str(row[0])) if row and row[0] else None
                except (TypeError, ValueError):
                    conn.rollback()
                    return result("state_changed")
                pet_uid = str(expected_travel.get("pet_uid", ""))
                pet_table = conn.execute(
                    "SELECT 1 FROM player_data.sqlite_master WHERE type='table' AND name=%s", ("player_pet_item",)
                ).fetchone()
                if pet_table is not None and (not pet_uid or conn.execute(
                    "SELECT 1 FROM player_data.player_pet_item WHERE user_id=%s AND uid=%s", (user_id, pet_uid)
                ).fetchone() is None):
                    conn.rollback()
                    return result("pet_missing")
                current_json = json.dumps(current_travel, ensure_ascii=False, sort_keys=True, separators=(",", ":"))
                if current_json != travel_json:
                    conn.rollback()
                    return result("state_changed")
                for item_id, _, _, amount in rewards:
                    inventory = conn.execute(
                        "SELECT COALESCE(goods_num, 0) FROM back WHERE user_id=%s AND goods_id=%s", (user_id, item_id)
                    ).fetchone()
                    if (int(inventory[0]) if inventory else 0) + amount > max_goods_num:
                        conn.rollback()
                        return result("inventory_full")

                if conn.execute(
                    "UPDATE player_data.player_pet SET travel=NULL WHERE user_id=%s AND travel=%s", (user_id, row[0])
                ).rowcount != 1:
                    conn.rollback()
                    return result("state_changed")
                conn.execute("UPDATE user_xiuxian SET stone=stone+%s, exp=exp+%s WHERE user_id=%s", (stone, exp, user_id))
                now = datetime.now()
                for item_id, name, item_type, amount in rewards:
                    conn.execute(
                        "INSERT INTO back (user_id, goods_id, goods_name, goods_type, goods_num, create_time, update_time, bind_num) "
                        "VALUES (%s,%s,%s,%s,%s,%s,%s,%s) ON CONFLICT (user_id, goods_id) DO UPDATE SET "
                        "goods_name=EXCLUDED.goods_name, goods_type=EXCLUDED.goods_type, update_time=EXCLUDED.update_time, "
                        "goods_num=back.goods_num+EXCLUDED.goods_num, bind_num=COALESCE(back.bind_num,0)+EXCLUDED.goods_num",
                        (user_id, item_id, name, item_type, amount, now, now, amount),
                    )
                conn.execute("INSERT INTO pet_travel_claim_operations VALUES (%s, %s, CURRENT_TIMESTAMP)", (operation_id, payload))
                conn.commit()
                return result("applied")
            except Exception:
                conn.rollback()
                raise
            finally:
                if attached:
                    conn.execute("DETACH DATABASE player_data")

@dataclass(frozen=True)
class PetFeedResult:
    status: str
    stars: int = 0
    exp: int = 0
    total_exp: int = 0

    @property
    def succeeded(self):
        return self.status in {"applied", "duplicate"}

class PetFeedService:
    def __init__(self, game_db: str | Path, player_db: str | Path, lock: RLock | None = None):
        self.game_db = Path(game_db)
        self.player_db = Path(player_db)
        self.lock = lock or RLock()

    def feed(self, operation_id, user_id, uid, item_id, count, expected, updated):
        operation_id, user_id, uid = map(str, (operation_id, user_id, uid))
        item_id, count = int(item_id), int(count)
        expected = tuple(map(int, expected))
        updated = tuple(map(int, updated))
        # Request identity only — exp/stars snapshots are concurrency checks.
        payload = json.dumps([user_id, uid, item_id, count], ensure_ascii=True, separators=(",", ":"))
        with self.lock, closing(db_backend.connect(self.game_db)) as conn:
            try:
                conn.execute("ATTACH DATABASE %s AS player_data", (str(self.player_db),))
                conn.execute("BEGIN IMMEDIATE")
                conn.execute("CREATE TABLE IF NOT EXISTS pet_feed_operations(operation_id TEXT PRIMARY KEY,payload TEXT,stars INTEGER,exp INTEGER,total_exp INTEGER)")
                old = conn.execute("SELECT payload,stars,exp,total_exp FROM pet_feed_operations WHERE operation_id=%s", (operation_id,)).fetchone()
                if old:
                    conn.rollback()
                    return PetFeedResult("duplicate" if old[0] == payload else "state_changed", *map(int, old[1:]))
                pet = conn.execute("SELECT stars,exp,total_exp,is_active FROM player_data.player_pet_item WHERE user_id=%s AND uid=%s", (user_id, uid)).fetchone()
                if pet is None or tuple(map(int, pet[:3])) != expected or int(pet[3]) != 1:
                    conn.rollback(); return PetFeedResult("state_changed")
                item = conn.execute("SELECT goods_num FROM back WHERE user_id=%s AND goods_id=%s", (user_id, item_id)).fetchone()
                if item is None or int(item[0]) < count:
                    conn.rollback(); return PetFeedResult("item_missing")
                conn.execute("UPDATE back SET goods_num=goods_num-%s WHERE user_id=%s AND goods_id=%s", (count, user_id, item_id))
                conn.execute("UPDATE player_data.player_pet_item SET stars=%s,exp=%s,total_exp=%s,updated_at=strftime('%%s','now') WHERE user_id=%s AND uid=%s", (*updated, user_id, uid))
                conn.execute("INSERT INTO pet_feed_operations VALUES (%s,%s,%s,%s,%s)", (operation_id, payload, *updated))
                conn.commit()
                return PetFeedResult("applied", *updated)
            except Exception:
                conn.rollback(); raise

@dataclass(frozen=True)
class PetSkillReplaceResult:
    status: str
    skill_id: str = ""
    @property
    def succeeded(self): return self.status in {"applied", "duplicate"}

class PetSkillReplaceService:
    def __init__(self, player_db: str | Path, lock: RLock | None = None): self.db=Path(player_db); self.lock=lock or RLock()
    def replace(self, operation_id, user_id, uid, expected_skill_id, new_skill_id):
        values=tuple(map(str,(user_id,uid,expected_skill_id,new_skill_id))); operation_id=str(operation_id); payload=json.dumps(values)
        with self.lock, closing(db_backend.connect(self.db)) as conn:
            try:
                conn.execute("BEGIN IMMEDIATE"); conn.execute("CREATE TABLE IF NOT EXISTS pet_skill_replace_operations(operation_id TEXT PRIMARY KEY,payload TEXT,skill_id TEXT)")
                old=conn.execute("SELECT payload,skill_id FROM pet_skill_replace_operations WHERE operation_id=%s",(operation_id,)).fetchone()
                if old: conn.rollback(); return PetSkillReplaceResult("duplicate" if old[0]==payload else "state_changed", str(old[1]) if old[0]==payload else "")
                row=conn.execute("SELECT COALESCE(skill_id,'') FROM player_pet_item WHERE user_id=%s AND uid=%s",values[:2]).fetchone()
                if row is None or str(row[0])!=values[2]: conn.rollback(); return PetSkillReplaceResult("state_changed")
                conn.execute("UPDATE player_pet_item SET skill_id=%s,updated_at=strftime('%%s','now') WHERE user_id=%s AND uid=%s",(values[3],values[0],values[1]))
                conn.execute("INSERT INTO pet_skill_replace_operations VALUES (%s,%s,%s)",(operation_id,payload,values[3])); conn.commit(); return PetSkillReplaceResult("applied",values[3])
            except Exception: conn.rollback(); raise

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

    def release(
        self,
        operation_id,
        user_id,
        uid,
        expected_exp,
        refund_item,
        refund,
        max_goods,
        expected_is_active=True,
    ) -> PetReleaseResult:
        return self.release_batch(
            operation_id,
            user_id,
            [{"uid": uid, "total_exp": expected_exp, "is_active": int(bool(expected_is_active))}],
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

                active_uids = tuple(pet[0] for pet in pets if pet[2])
                if active_uids:
                    current_active = conn.execute(
                        "SELECT uid FROM player_data.player_pet_item "
                        "WHERE user_id=%s AND COALESCE(is_active,0)=1 ORDER BY uid",
                        (user_id,),
                    ).fetchall()
                    meta = conn.execute(
                        "SELECT active_uid FROM player_data.player_pet WHERE user_id=%s",
                        (user_id,),
                    ).fetchone()
                    if (
                        tuple(str(row[0]) for row in current_active) != active_uids
                        or meta is None
                        or str(meta[0] or "") != active_uids[0]
                    ):
                        conn.rollback()
                        return PetReleaseResult("state_changed")

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
                if active_uids:
                    cleared = conn.execute(
                        "UPDATE player_data.player_pet SET active_uid=NULL,active=NULL "
                        "WHERE user_id=%s AND active_uid=%s",
                        (user_id, active_uids[0]),
                    )
                    if cleared.rowcount != 1:
                        raise RuntimeError("active pet metadata changed")
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

@dataclass(frozen=True)
class PetFusionBreakthroughResult:
    status: str
    stars: int = 0
    exp: int = 0

    @property
    def succeeded(self) -> bool:
        return self.status in {"applied", "duplicate"}

class PetFusionBreakthroughService:
    """Consume fixed pet materials and promote the active pet atomically."""

    def __init__(self, player_database: str | Path, lock: RLock | None = None) -> None:
        self._database = Path(player_database)
        self._lock = lock or RLock()

    def breakthrough(
        self,
        operation_id,
        user_id,
        expected_main,
        expected_materials,
        updated_stars,
        updated_exp,
        skill_offer=None,
    ) -> PetFusionBreakthroughResult:
        operation_id = str(operation_id).strip()
        user_id = str(user_id)
        main = tuple(expected_main)
        materials = tuple(sorted(tuple(row) for row in expected_materials))
        updated_stars, updated_exp = int(updated_stars), int(updated_exp)
        if not operation_id or len(main) != 7 or not materials:
            raise ValueError("operation, main snapshot and materials are required")
        if any(len(row) != 7 for row in materials) or len({str(row[0]) for row in materials}) != len(materials):
            raise ValueError("unique complete material snapshots are required")
        payload = json.dumps(
            [user_id, main, materials, updated_stars, updated_exp, skill_offer],
            ensure_ascii=True,
            sort_keys=True,
            separators=(",", ":"),
        )

        with self._lock, closing(db_backend.connect(self._database)) as conn:
            try:
                conn.execute("BEGIN IMMEDIATE")
                conn.execute(
                    "CREATE TABLE IF NOT EXISTS pet_fusion_breakthrough_operations ("
                    "operation_id TEXT PRIMARY KEY,payload TEXT NOT NULL,stars INTEGER NOT NULL,"
                    "exp INTEGER NOT NULL,created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP)"
                )
                old = conn.execute(
                    "SELECT payload,stars,exp FROM pet_fusion_breakthrough_operations WHERE operation_id=%s",
                    (operation_id,),
                ).fetchone()
                if old is not None:
                    conn.rollback()
                    if str(old[0]) != payload:
                        return PetFusionBreakthroughResult("state_changed")
                    return PetFusionBreakthroughResult("duplicate", int(old[1]), int(old[2]))

                current_main = conn.execute(
                    "SELECT uid,pet_id,stars,exp,total_exp,COALESCE(skill_id,''),is_active "
                    "FROM player_pet_item WHERE user_id=%s AND uid=%s",
                    (user_id, str(main[0])),
                ).fetchone()
                if current_main is None or tuple(current_main) != main or int(current_main[6]) != 1:
                    conn.rollback()
                    return PetFusionBreakthroughResult("state_changed")

                placeholders = ",".join(["%s"] * len(materials))
                material_uids = tuple(str(row[0]) for row in materials)
                rows = conn.execute(
                    "SELECT uid,pet_id,stars,exp,total_exp,COALESCE(skill_id,''),is_active "
                    f"FROM player_pet_item WHERE user_id=%s AND uid IN ({placeholders})",
                    (user_id, *material_uids),
                ).fetchall()
                current_materials = tuple(sorted(tuple(row) for row in rows))
                if current_materials != materials or any(int(row[6]) for row in current_materials):
                    conn.rollback()
                    return PetFusionBreakthroughResult("state_changed")

                deleted = conn.execute(
                    f"DELETE FROM player_pet_item WHERE user_id=%s AND uid IN ({placeholders})",
                    (user_id, *material_uids),
                ).rowcount
                if deleted != len(materials):
                    conn.rollback()
                    return PetFusionBreakthroughResult("state_changed")
                updated = conn.execute(
                    "UPDATE player_pet_item SET stars=%s,exp=%s,updated_at=%s "
                    "WHERE user_id=%s AND uid=%s AND stars=%s AND exp=%s AND is_active=1",
                    (updated_stars, updated_exp, int(time.time()), user_id, str(main[0]), int(main[2]), int(main[3])),
                ).rowcount
                if updated != 1:
                    conn.rollback()
                    return PetFusionBreakthroughResult("state_changed")
                conn.execute(
                    "INSERT INTO pet_fusion_breakthrough_operations(operation_id,payload,stars,exp) "
                    "VALUES (%s,%s,%s,%s)",
                    (operation_id, payload, updated_stars, updated_exp),
                )
                conn.commit()
                return PetFusionBreakthroughResult("applied", updated_stars, updated_exp)
            except Exception:
                conn.rollback()
                raise

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

__all__ = [
    "PetTravelClaimResult",
    "PetTravelClaimService",
    "PetFeedResult",
    "PetFeedService",
    "PetSkillReplaceResult",
    "PetSkillReplaceService",
    "PetTravelStartResult",
    "PetTravelStartService",
    "PetHatchResult",
    "PetHatchService",
    "PetReleaseResult",
    "PetReleaseService",
    "PetFusionBreakthroughResult",
    "PetFusionBreakthroughService",
    "PetSkillRerollResult",
    "PetSkillRerollService",
    "PetActiveSwitchResult",
    "PetActiveSwitchService",
]
