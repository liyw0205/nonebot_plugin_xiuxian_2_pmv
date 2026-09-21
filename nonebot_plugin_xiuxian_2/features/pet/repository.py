from __future__ import annotations

import json
import time
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Protocol

from ...infrastructure.database import DatabaseUnitOfWork


@dataclass(frozen=True)
class PetActiveSwitchResult:
    status: str
    active_uid: str = ""

    @property
    def succeeded(self) -> bool:
        return self.status in {"applied", "duplicate", "already_active"}


class PetActiveSwitchSqlRepository:
    def __init__(self, database: str | Path) -> None:
        self.database = str(database)

    def switch(self, operation_id: str, user_id: str, expected_active_uid: str, target_uid: str, travel_pet_uid: str = "") -> PetActiveSwitchResult:
        operation_id = str(operation_id).strip()
        user_id = str(user_id)
        expected_active_uid = str(expected_active_uid or "")
        target_uid = str(target_uid).strip()
        travel_pet_uid = str(travel_pet_uid or "")
        if not operation_id or not user_id or not target_uid:
            raise ValueError("operation, user and target pet are required")
        payload = json.dumps([user_id, target_uid], ensure_ascii=True, separators=(",", ":"))
        with DatabaseUnitOfWork(self.database, immediate=True) as uow:
            uow.execute(
                "CREATE TABLE IF NOT EXISTS pet_active_switch_operations("
                "operation_id TEXT PRIMARY KEY,payload TEXT NOT NULL,active_uid TEXT NOT NULL,"
                "created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP)"
            )
            previous = uow.query_one(
                "SELECT payload,active_uid FROM pet_active_switch_operations WHERE operation_id=?",
                (operation_id,),
            )
            if previous is not None:
                if str(previous["payload"]) != payload:
                    return PetActiveSwitchResult("operation_conflict")
                return PetActiveSwitchResult("duplicate", str(previous["active_uid"]))
            target = uow.query_one(
                "SELECT COALESCE(is_active,0) AS is_active FROM player_pet_item WHERE user_id=? AND uid=?",
                (user_id, target_uid),
            )
            if target is None:
                return PetActiveSwitchResult("pet_missing")
            current = uow.query_all(
                "SELECT uid FROM player_pet_item WHERE user_id=? AND COALESCE(is_active,0)=1",
                (user_id,),
            )
            current_uids = tuple(str(row["uid"]) for row in current)
            actual_active = current_uids[0] if len(current_uids) == 1 else ""
            if len(current_uids) > 1 or actual_active != expected_active_uid:
                return PetActiveSwitchResult("state_changed")
            if target_uid == actual_active:
                return PetActiveSwitchResult("already_active", target_uid)
            if target_uid == travel_pet_uid:
                return PetActiveSwitchResult("pet_traveling")
            meta = uow.query_one("SELECT active_uid FROM player_pet WHERE user_id=?", (user_id,))
            if meta is None or str(meta["active_uid"] or "") != expected_active_uid:
                return PetActiveSwitchResult("state_changed")
            now = int(time.time())
            uow.execute(
                "UPDATE player_pet_item SET is_active=0,updated_at=? WHERE user_id=? AND COALESCE(is_active,0)=1",
                (now, user_id),
            )
            changed = uow.execute(
                "UPDATE player_pet_item SET is_active=1,updated_at=? WHERE user_id=? AND uid=? AND COALESCE(is_active,0)=0",
                (now, user_id, target_uid),
            )
            if changed.rowcount != 1:
                raise RuntimeError("target pet state changed")
            changed = uow.execute(
                "UPDATE player_pet SET active_uid=?,active=? WHERE user_id=? AND COALESCE(active_uid,'')=?",
                (target_uid, target_uid, user_id, expected_active_uid),
            )
            if changed.rowcount != 1:
                raise RuntimeError("pet metadata state changed")
            uow.execute(
                "INSERT INTO pet_active_switch_operations(operation_id,payload,active_uid) VALUES(?,?,?)",
                (operation_id, payload, target_uid),
            )
            return PetActiveSwitchResult("applied", target_uid)


@dataclass(frozen=True)
class PetFeedResult:
    status: str
    stars: int = 0
    exp: int = 0
    total_exp: int = 0

    @property
    def succeeded(self) -> bool:
        return self.status in {"applied", "duplicate"}


class PetFeedSqlRepository:
    def __init__(self, game_database: str | Path, player_database: str | Path) -> None:
        self.game_database = str(game_database)
        self.player_database = str(player_database)

    def feed(self, operation_id: str, user_id: str, uid: str, item_id: int, count: int, expected: tuple[int, ...], updated: tuple[int, ...]) -> PetFeedResult:
        operation_id, user_id, uid = str(operation_id), str(user_id), str(uid)
        item_id, count = int(item_id), int(count)
        expected, updated = tuple(map(int, expected)), tuple(map(int, updated))
        if not operation_id or not user_id or not uid or len(expected) != 3 or len(updated) != 3 or count < 1:
            raise ValueError("valid feed operation, pet and snapshots are required")
        payload = json.dumps([user_id, uid, item_id, count], ensure_ascii=True, separators=(",", ":"))
        with DatabaseUnitOfWork(self.game_database, immediate=True) as uow:
            uow.attach_database(self.player_database, "player_data")
            uow.execute(
                "CREATE TABLE IF NOT EXISTS pet_feed_operations("
                "operation_id TEXT PRIMARY KEY,payload TEXT,stars INTEGER,exp INTEGER,total_exp INTEGER)"
            )
            old = uow.query_one(
                "SELECT payload,stars,exp,total_exp FROM pet_feed_operations WHERE operation_id=?",
                (operation_id,),
            )
            if old is not None:
                return PetFeedResult(
                    "duplicate" if str(old["payload"]) == payload else "state_changed",
                    int(old["stars"]), int(old["exp"]), int(old["total_exp"]),
                )
            pet = uow.query_one(
                "SELECT stars,exp,total_exp,is_active FROM player_data.player_pet_item WHERE user_id=? AND uid=?",
                (user_id, uid),
            )
            if pet is None or tuple(int(pet[field]) for field in ("stars", "exp", "total_exp")) != expected or int(pet["is_active"]) != 1:
                return PetFeedResult("state_changed")
            item = uow.query_one("SELECT goods_num FROM back WHERE user_id=? AND goods_id=?", (user_id, item_id))
            if item is None or int(item["goods_num"]) < count:
                return PetFeedResult("item_missing")
            uow.execute("UPDATE back SET goods_num=goods_num-? WHERE user_id=? AND goods_id=?", (count, user_id, item_id))
            uow.execute(
                "UPDATE player_data.player_pet_item SET stars=?,exp=?,total_exp=?,updated_at=strftime('%s','now') WHERE user_id=? AND uid=?",
                (*updated, user_id, uid),
            )
            uow.execute(
                "INSERT INTO pet_feed_operations(operation_id,payload,stars,exp,total_exp) VALUES(?,?,?,?,?)",
                (operation_id, payload, *updated),
            )
            return PetFeedResult("applied", *updated)


@dataclass(frozen=True)
class PetTravelStartResult:
    status: str

    @property
    def succeeded(self) -> bool:
        return self.status in {"applied", "duplicate"}


class PetTravelStartSqlRepository:
    def __init__(self, database: str | Path) -> None:
        self.database = str(database)

    def start(self, operation_id: str, user_id: str, pet_uid: str, expected_travel: dict[str, Any] | None, travel: dict[str, Any]) -> PetTravelStartResult:
        operation_id, user_id, pet_uid = str(operation_id).strip(), str(user_id), str(pet_uid)
        if not operation_id or not user_id or not pet_uid or not isinstance(travel, dict):
            raise ValueError("operation, user, pet and travel are required")
        expected_json = None if expected_travel is None else json.dumps(expected_travel, ensure_ascii=False, sort_keys=True)
        travel_json = json.dumps(travel, ensure_ascii=False, sort_keys=True)
        payload = json.dumps([user_id, pet_uid, expected_json, travel_json], ensure_ascii=True)
        with DatabaseUnitOfWork(self.database, immediate=True) as uow:
            uow.execute(
                "CREATE TABLE IF NOT EXISTS pet_travel_start_operations("
                "operation_id TEXT PRIMARY KEY,payload TEXT NOT NULL,created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP)"
            )
            previous = uow.query_one("SELECT payload FROM pet_travel_start_operations WHERE operation_id=?", (operation_id,))
            if previous is not None:
                return PetTravelStartResult("duplicate" if str(previous["payload"]) == payload else "state_changed")
            meta = uow.query_one("SELECT travel FROM player_pet WHERE user_id=?", (user_id,))
            if meta is None:
                return PetTravelStartResult("user_missing")
            current = None if meta["travel"] is None else json.dumps(json.loads(str(meta["travel"])), ensure_ascii=False, sort_keys=True)
            if current != expected_json:
                return PetTravelStartResult("state_changed")
            pet = uow.query_one("SELECT is_active FROM player_pet_item WHERE user_id=? AND uid=?", (user_id, pet_uid))
            if pet is None or int(pet["is_active"]) != 1:
                return PetTravelStartResult("pet_changed")
            if uow.execute("UPDATE player_pet SET travel=? WHERE user_id=? AND travel IS NULL", (travel_json, user_id)).rowcount != 1:
                return PetTravelStartResult("state_changed")
            uow.execute("INSERT INTO pet_travel_start_operations(operation_id,payload) VALUES(?,?)", (operation_id, payload))
            return PetTravelStartResult("applied")


@dataclass(frozen=True)
class PetTravelClaimResult:
    status: str
    stone: int = 0
    exp: int = 0
    items: tuple[tuple[int, int], ...] = ()

    @property
    def succeeded(self) -> bool:
        return self.status in {"applied", "duplicate"}


class PetTravelClaimSqlRepository:
    def __init__(self, game_database: str | Path, player_database: str | Path) -> None:
        self.game_database = str(game_database)
        self.player_database = str(player_database)

    def claim(self, operation_id: str, user_id: str, expected_travel: dict[str, Any], stone: int, exp: int, items: Any, max_goods_num: int) -> PetTravelClaimResult:
        operation_id, user_id = str(operation_id).strip(), str(user_id)
        stone, exp, max_goods_num = int(stone), int(exp), int(max_goods_num)
        rewards = tuple(
            (int(item["id"]), str(item["name"]), str(item["type"]), int(item["amount"]))
            for item in items
            if int(item.get("id", 0)) > 0 and int(item.get("amount", 0)) > 0
        )
        if not operation_id or not isinstance(expected_travel, dict) or min(stone, exp, max_goods_num) < 0:
            raise ValueError("valid operation, travel and rewards are required")
        travel_json = json.dumps(expected_travel, ensure_ascii=False, sort_keys=True, separators=(",", ":"))
        payload = json.dumps([user_id, expected_travel, stone, exp, rewards, max_goods_num], ensure_ascii=True, sort_keys=True)
        with DatabaseUnitOfWork(self.game_database, immediate=True) as uow:
            uow.attach_database(self.player_database, "player_data")
            uow.execute(
                "CREATE TABLE IF NOT EXISTS pet_travel_claim_operations("
                "operation_id TEXT PRIMARY KEY,payload TEXT NOT NULL,created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP)"
            )
            previous = uow.query_one("SELECT payload FROM pet_travel_claim_operations WHERE operation_id=?", (operation_id,))
            if previous is not None:
                return PetTravelClaimResult("duplicate" if str(previous["payload"]) == payload else "state_changed", stone if str(previous["payload"]) == payload else 0, exp if str(previous["payload"]) == payload else 0, tuple((row[0], row[3]) for row in rewards) if str(previous["payload"]) == payload else ())
            if uow.query_one("SELECT 1 AS present FROM user_xiuxian WHERE user_id=?", (user_id,)) is None:
                return PetTravelClaimResult("user_missing")
            meta = uow.query_one("SELECT travel FROM player_data.player_pet WHERE user_id=?", (user_id,))
            if meta is None:
                return PetTravelClaimResult("state_changed")
            current = None if meta["travel"] is None else json.dumps(json.loads(str(meta["travel"])), ensure_ascii=False, sort_keys=True, separators=(",", ":"))
            if current != travel_json:
                return PetTravelClaimResult("state_changed")
            pet_uid = str(expected_travel.get("pet_uid", ""))
            if uow.query_one("SELECT 1 AS present FROM player_data.player_pet_item WHERE user_id=? AND uid=?", (user_id, pet_uid)) is None:
                return PetTravelClaimResult("pet_missing")
            for item_id, _, _, amount in rewards:
                inventory = uow.query_one("SELECT COALESCE(goods_num,0) AS goods_num FROM back WHERE user_id=? AND goods_id=?", (user_id, item_id))
                if (int(inventory["goods_num"]) if inventory else 0) + amount > max_goods_num:
                    return PetTravelClaimResult("inventory_full")
            if uow.execute("UPDATE player_data.player_pet SET travel=NULL WHERE user_id=? AND travel=?", (user_id, meta["travel"])).rowcount != 1:
                return PetTravelClaimResult("state_changed")
            uow.execute("UPDATE user_xiuxian SET stone=COALESCE(stone,0)+?,exp=COALESCE(exp,0)+? WHERE user_id=?", (stone, exp, user_id))
            for item_id, name, item_type, amount in rewards:
                uow.execute(
                    "INSERT INTO back(user_id,goods_id,goods_name,goods_type,goods_num,bind_num) VALUES(?,?,?,?,?,?) "
                    "ON CONFLICT(user_id,goods_id) DO UPDATE SET goods_name=excluded.goods_name,goods_type=excluded.goods_type,goods_num=back.goods_num+excluded.goods_num,bind_num=COALESCE(back.bind_num,0)+excluded.goods_num",
                    (user_id, item_id, name, item_type, amount, amount),
                )
            uow.execute("INSERT INTO pet_travel_claim_operations(operation_id,payload) VALUES(?,?)", (operation_id, payload))
            return PetTravelClaimResult("applied", stone, exp, tuple((row[0], row[3]) for row in rewards))


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


class PetHatchSqlRepository:
    def __init__(self, game_database: str | Path, player_database: str | Path) -> None:
        self.game_database = str(game_database)
        self.player_database = str(player_database)

    def get_result(self, operation_id: str) -> PetHatchResult | None:
        operation_id = str(operation_id).strip()
        if not operation_id:
            return None
        with DatabaseUnitOfWork(self.game_database) as uow:
            uow.execute("CREATE TABLE IF NOT EXISTS pet_hatch_operations(operation_id TEXT PRIMARY KEY,payload TEXT,result_json TEXT,created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP)")
            row = uow.query_one("SELECT payload,result_json FROM pet_hatch_operations WHERE operation_id=?", (operation_id,))
        if row is None:
            return None
        data = json.loads(row["result_json"] or "{}")
        return PetHatchResult("duplicate", 0, tuple((dict(pet), bool(active)) for pet, active in data.get("pets", [])), tuple(data.get("updated_meta", [])), int(data.get("bag_limit", 0) or 0))

    @staticmethod
    def _normalize_travel(value: Any) -> Any:
        if value is None or value == "":
            return None
        if isinstance(value, dict):
            return json.dumps(value, ensure_ascii=False, sort_keys=True, separators=(",", ":")) if value else None
        try:
            parsed = json.loads(str(value))
        except (TypeError, ValueError):
            return str(value)
        return None if parsed is None or parsed == {} else json.dumps(parsed, ensure_ascii=False, sort_keys=True, separators=(",", ":"))

    def hatch(self, operation_id: str, user_id: str, expected_stone: int, cost: int, expected_meta: Any, pets: Any, updated_meta: Any, bag_limit: int) -> PetHatchResult:
        operation_id, user_id = str(operation_id).strip(), str(user_id)
        expected_stone, cost, bag_limit = int(expected_stone), int(cost), int(bag_limit)
        normalized = tuple((dict(pet), bool(active)) for pet, active in pets)
        if not operation_id or cost < 0 or not normalized:
            raise ValueError("valid operation and hatch batch are required")
        payload = json.dumps([user_id, cost, len(normalized)], ensure_ascii=True, separators=(",", ":"))
        result_json = json.dumps({"pets": [[pet, active] for pet, active in normalized], "updated_meta": list(updated_meta), "bag_limit": bag_limit}, ensure_ascii=True, separators=(",", ":"))
        with DatabaseUnitOfWork(self.game_database, immediate=True) as uow:
            uow.attach_database(self.player_database, "player_data")
            uow.execute("CREATE TABLE IF NOT EXISTS pet_hatch_operations(operation_id TEXT PRIMARY KEY,payload TEXT,result_json TEXT,created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP)")
            columns = {str(row["name"]) for row in uow.query_all("PRAGMA table_info(pet_hatch_operations)")}
            if "result_json" not in columns:
                uow.execute("ALTER TABLE pet_hatch_operations ADD COLUMN result_json TEXT")
            old = uow.query_one("SELECT payload,result_json FROM pet_hatch_operations WHERE operation_id=?", (operation_id,))
            if old is not None:
                if str(old["payload"]) != payload:
                    return PetHatchResult("state_changed")
                data = json.loads(old["result_json"] or "{}")
                return PetHatchResult("duplicate", cost, tuple((dict(pet), bool(active)) for pet, active in data.get("pets", [])), tuple(data.get("updated_meta", [])), int(data.get("bag_limit", bag_limit)))
            user = uow.query_one("SELECT stone FROM user_xiuxian WHERE user_id=?", (user_id,))
            if user is None:
                return PetHatchResult("user_missing")
            if int(user["stone"] or 0) != expected_stone:
                return PetHatchResult("state_changed")
            if expected_stone < cost:
                return PetHatchResult("stone_missing")
            meta = uow.query_one("SELECT active_uid,egg_pity_count,egg_pity_no_mythic_count,travel FROM player_data.player_pet WHERE user_id=?", (user_id,))
            expected = list(expected_meta or []) + [None] * 4
            actual = ["" if meta is None else str(meta["active_uid"] or ""), 0 if meta is None else int(meta["egg_pity_count"] or 0), 0 if meta is None else int(meta["egg_pity_no_mythic_count"] or 0), None if meta is None else self._normalize_travel(meta["travel"])]
            expected[0], expected[3] = str(expected[0] or ""), self._normalize_travel(expected[3])
            if actual != expected[:4]:
                return PetHatchResult("state_changed")
            owned = uow.query_one("SELECT COUNT(*) AS count FROM player_data.player_pet_item WHERE user_id=?", (user_id,))
            if int(owned["count"]) + len(normalized) > bag_limit:
                return PetHatchResult("inventory_full")
            now = int(time.time())
            for pet, active in normalized:
                skill = pet.get("skill") or ((pet.get("skills") or [{}])[0])
                uow.execute("INSERT INTO player_data.player_pet_item(id,user_id,uid,is_active,pet_id,stars,exp,total_exp,skill_id,created_at,updated_at) VALUES(?,?,?,?,?,?,?,?,?,?,?)", (f"{user_id}:{pet['uid']}", user_id, str(pet["uid"]), int(active), str(pet.get("pet_id", "")), int(pet.get("stars", 1)), int(pet.get("exp", 0)), int(pet.get("total_exp", 0)), str(skill.get("skill_id", "")) or None, now, now))
            if uow.execute("UPDATE user_xiuxian SET stone=stone-? WHERE user_id=? AND stone=?", (cost, user_id, expected_stone)).rowcount != 1:
                return PetHatchResult("state_changed")
            if meta is None:
                uow.execute("INSERT INTO player_data.player_pet(user_id,active_uid,egg_pity_count,egg_pity_no_mythic_count,travel) VALUES(?,?,?,?,NULL)", (user_id, updated_meta[0], updated_meta[1], updated_meta[2]))
            elif uow.execute("UPDATE player_data.player_pet SET active_uid=?,egg_pity_count=?,egg_pity_no_mythic_count=? WHERE user_id=?", (*updated_meta, user_id)).rowcount != 1:
                return PetHatchResult("state_changed")
            uow.execute("INSERT INTO pet_hatch_operations(operation_id,payload,result_json) VALUES(?,?,?)", (operation_id, payload, result_json))
            return PetHatchResult("applied", cost, normalized, tuple(updated_meta), bag_limit)


class PetRepository(Protocol):
    def switch(self, *args: Any, **kwargs: Any) -> Any: ...
    def travel_claim(self, *args: Any, **kwargs: Any) -> Any: ...
    def travel_start(self, *args: Any, **kwargs: Any) -> Any: ...
    def feed(self, *args: Any, **kwargs: Any) -> Any: ...
    def hatch(self, *args: Any, **kwargs: Any) -> Any: ...
    def hatch_result(self, *args: Any, **kwargs: Any) -> Any: ...


class LegacyPetRepository:
    def __init__(self, game_database: str | Path, player_database: str | Path) -> None:
        self.game_database = str(game_database)
        self.player_database = str(player_database)

    def _services(self):
        from ...xiuxian.xiuxian_pet.transaction_service import PetTravelClaimService, PetTravelStartService, PetFeedService, PetHatchService

        return (PetTravelClaimService(self.game_database, self.player_database), PetTravelStartService(self.player_database), PetFeedService(self.game_database, self.player_database), PetHatchService(self.game_database, self.player_database))

    def travel_claim(self, *args: Any, **kwargs: Any) -> Any:
        return self._services()[0].claim(*args, **kwargs)

    def travel_start(self, *args: Any, **kwargs: Any) -> Any:
        return self._services()[1].start(*args, **kwargs)

    def feed(self, *args: Any, **kwargs: Any) -> Any:
        return self._services()[2].feed(*args, **kwargs)

    def hatch(self, *args: Any, **kwargs: Any) -> Any:
        return self._services()[3].hatch(*args, **kwargs)

    def hatch_result(self, *args: Any, **kwargs: Any) -> Any:
        return self._services()[3].get_result(*args, **kwargs)

    def switch(self, *args: Any, **kwargs: Any) -> Any:
        from ...xiuxian.xiuxian_pet.transaction_service import PetActiveSwitchService

        return PetActiveSwitchService(self.player_database).switch(*args, **kwargs)


__all__ = ["PetActiveSwitchResult", "PetActiveSwitchSqlRepository", "PetFeedResult", "PetFeedSqlRepository", "PetTravelStartResult", "PetTravelStartSqlRepository", "PetTravelClaimResult", "PetTravelClaimSqlRepository", "PetHatchResult", "PetHatchSqlRepository", "PetRepository", "LegacyPetRepository"]
