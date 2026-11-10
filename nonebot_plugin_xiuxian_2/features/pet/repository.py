from __future__ import annotations

from ...core.numeric import as_int_like
from ...core.errors import DomainError

import json
import sqlite3
import time
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Protocol

from ...infrastructure.clock import SystemClock
from ...infrastructure.database import DatabaseUnitOfWork, OutboxStore
from .receipt_codec import ReceiptDecodeError, decode_receipt, encode_receipt
from .schema import (
    ACTIVE_SWITCH_COLUMNS,
    FEED_COLUMNS,
    FUSION_COLUMNS,
    RELEASE_COLUMNS,
    REROLL_COLUMNS,
    SCHEMA_MISSING,
    TRAVEL_START_COLUMNS,
    operations_ready,
)


def _pet_snapshot_reason(current, expected):
    reasons = ("pet_changed", "pet_species_changed", "pet_stars_changed", "pet_exp_changed", "pet_total_exp_changed", "skill_changed", "active_changed")
    return next(reason for left, right, reason in zip(current, expected, reasons) if left != right)


@dataclass(frozen=True)
class PetActiveSwitchResult:
    status: str
    active_uid: str = ""
    reason: str = ""

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
            if not operations_ready(uow, "pet_active_switch_operations", ACTIVE_SWITCH_COLUMNS):
                return PetActiveSwitchResult(SCHEMA_MISSING)
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
            if len(current_uids) > 1:
                return PetActiveSwitchResult("state_changed", reason="active_inconsistent")
            if actual_active != expected_active_uid:
                return PetActiveSwitchResult("state_changed", reason="active_changed")
            if target_uid == actual_active:
                return PetActiveSwitchResult("already_active", target_uid)
            if target_uid == travel_pet_uid:
                return PetActiveSwitchResult("pet_traveling")
            meta = uow.query_one("SELECT active_uid FROM player_pet WHERE user_id=?", (user_id,))
            if meta is None:
                return PetActiveSwitchResult("state_changed", reason="meta_missing")
            if str(meta["active_uid"] or "") != expected_active_uid:
                return PetActiveSwitchResult("state_changed", reason="active_changed")
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
    reason: str = ""

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
            if not operations_ready(uow, "pet_feed_operations", FEED_COLUMNS):
                return PetFeedResult(SCHEMA_MISSING)
            old = uow.query_one(
                "SELECT payload,stars,exp,total_exp FROM pet_feed_operations WHERE operation_id=?",
                (operation_id,),
            )
            if old is not None:
                return PetFeedResult(
                    "duplicate" if str(old["payload"]) == payload else "state_changed",
                    int(old["stars"]), as_int_like(old["exp"]), as_int_like(old["total_exp"]),
                    reason="" if str(old["payload"]) == payload else "operation_conflict",
                )
            pet = uow.query_one(
                "SELECT stars,exp,total_exp,is_active FROM player_data.player_pet_item WHERE user_id=? AND uid=?",
                (user_id, uid),
            )
            if pet is None:
                return PetFeedResult("state_changed", reason="pet_missing")
            if int(pet["is_active"]) != 1:
                return PetFeedResult("state_changed", reason="pet_inactive")
            if tuple(int(pet[field]) for field in ("stars", "exp", "total_exp")) != expected:
                reason = next(reason for field, value, reason in zip(("stars", "exp", "total_exp"), expected, ("pet_stars_changed", "pet_exp_changed", "pet_total_exp_changed")) if int(pet[field]) != value)
                return PetFeedResult("state_changed", reason=reason)
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
    reason: str = ""

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
            if not operations_ready(uow, "pet_travel_start_operations", TRAVEL_START_COLUMNS):
                return PetTravelStartResult(SCHEMA_MISSING)
            previous = uow.query_one("SELECT payload FROM pet_travel_start_operations WHERE operation_id=?", (operation_id,))
            if previous is not None:
                return PetTravelStartResult("duplicate" if str(previous["payload"]) == payload else "state_changed", reason="" if str(previous["payload"]) == payload else "operation_conflict")
            meta = uow.query_one("SELECT travel FROM player_pet WHERE user_id=?", (user_id,))
            if meta is None:
                return PetTravelStartResult("user_missing")
            current = None if meta["travel"] is None else json.dumps(json.loads(str(meta["travel"])), ensure_ascii=False, sort_keys=True)
            if current != expected_json:
                return PetTravelStartResult("state_changed", reason="travel_changed")
            pet = uow.query_one("SELECT is_active FROM player_pet_item WHERE user_id=? AND uid=?", (user_id, pet_uid))
            if pet is None:
                return PetTravelStartResult("pet_changed", reason="pet_missing")
            if int(pet["is_active"]) != 1:
                return PetTravelStartResult("pet_changed", reason="pet_inactive")
            if uow.execute("UPDATE player_pet SET travel=? WHERE user_id=? AND travel IS NULL", (travel_json, user_id)).rowcount != 1:
                return PetTravelStartResult("state_changed", reason="travel_changed")
            uow.execute("INSERT INTO pet_travel_start_operations(operation_id,payload) VALUES(?,?)", (operation_id, payload))
            return PetTravelStartResult("applied")


@dataclass(frozen=True)
class PetTravelClaimResult:
    status: str
    stone: int = 0
    exp: int = 0
    items: tuple[tuple[int, int], ...] = ()
    effects_event_id: str | None = None
    reason: str = ""

    @property
    def succeeded(self) -> bool:
        return self.status in {"applied", "duplicate"}


class PetTravelClaimSqlRepository:
    def __init__(self, game_database: str | Path, player_database: str | Path, *, clock: Any | None = None) -> None:
        self.game_database = str(game_database)
        self.player_database = str(player_database)
        self.clock = clock or SystemClock()
        self.outbox = OutboxStore(clock=self.clock)

    @staticmethod
    def _schema_ready(uow: DatabaseUnitOfWork) -> bool:
        required = {
            "pet_travel_claim_operations": {"operation_id", "payload", "created_at"},
            "domain_outbox": {
                "event_id", "aggregate_type", "aggregate_id", "event_type", "payload_json",
                "status", "attempts", "next_attempt_at", "created_at", "updated_at",
            },
        }
        return all(
            columns <= {
                str(row["name"])
                for row in uow.query_all(f'PRAGMA table_info("{table}")')
            }
            for table, columns in required.items()
        )

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
        effects_event_id = f"pet.travel.effects:{operation_id}"
        with DatabaseUnitOfWork(self.game_database, immediate=True) as uow:
            uow.attach_database(self.player_database, "player_data")
            if not self._schema_ready(uow):
                return PetTravelClaimResult("schema_missing")
            previous = uow.query_one("SELECT payload FROM pet_travel_claim_operations WHERE operation_id=?", (operation_id,))
            if previous is not None:
                status = "duplicate" if str(previous["payload"]) == payload else "state_changed"
                event_id = (
                    effects_event_id
                    if status == "duplicate" and self.outbox.get(uow, effects_event_id) is not None
                    else None
                )
                return PetTravelClaimResult(status, stone if status == "duplicate" else 0, exp if status == "duplicate" else 0, tuple((row[0], row[3]) for row in rewards) if status == "duplicate" else (), event_id, reason="" if status == "duplicate" else "operation_conflict")
            if uow.query_one("SELECT 1 AS present FROM user_xiuxian WHERE user_id=?", (user_id,)) is None:
                return PetTravelClaimResult("user_missing")
            meta = uow.query_one("SELECT travel FROM player_data.player_pet WHERE user_id=?", (user_id,))
            if meta is None:
                return PetTravelClaimResult("state_changed", reason="meta_missing")
            current = None if meta["travel"] is None else json.dumps(json.loads(str(meta["travel"])), ensure_ascii=False, sort_keys=True, separators=(",", ":"))
            if current != travel_json:
                return PetTravelClaimResult("state_changed", reason="travel_changed")
            pet_uid = str(expected_travel.get("pet_uid", ""))
            if uow.query_one("SELECT 1 AS present FROM player_data.player_pet_item WHERE user_id=? AND uid=?", (user_id, pet_uid)) is None:
                return PetTravelClaimResult("pet_missing")
            for item_id, _, _, amount in rewards:
                inventory = uow.query_one("SELECT COALESCE(goods_num,0) AS goods_num FROM back WHERE user_id=? AND goods_id=?", (user_id, item_id))
                if (int(inventory["goods_num"]) if inventory else 0) + amount > max_goods_num:
                    return PetTravelClaimResult("inventory_full")
            if uow.execute("UPDATE player_data.player_pet SET travel=NULL WHERE user_id=? AND travel=?", (user_id, meta["travel"])).rowcount != 1:
                return PetTravelClaimResult("state_changed", reason="travel_changed")
            uow.execute("UPDATE user_xiuxian SET stone=COALESCE(stone,0)+?,exp=COALESCE(exp,0)+? WHERE user_id=?", (stone, exp, user_id))
            for item_id, name, item_type, amount in rewards:
                uow.execute(
                    "INSERT INTO back(user_id,goods_id,goods_name,goods_type,goods_num,bind_num) VALUES(?,?,?,?,?,?) "
                    "ON CONFLICT(user_id,goods_id) DO UPDATE SET goods_name=excluded.goods_name,goods_type=excluded.goods_type,goods_num=back.goods_num+excluded.goods_num,bind_num=COALESCE(back.bind_num,0)+excluded.goods_num",
                    (user_id, item_id, name, item_type, amount, amount),
                )
            uow.execute("INSERT INTO pet_travel_claim_operations(operation_id,payload) VALUES(?,?)", (operation_id, payload))
            occurred_at = self.clock.now().isoformat()
            travel = dict(expected_travel)
            self.outbox.append(
                uow,
                event_id=effects_event_id,
                aggregate_type="player",
                aggregate_id=user_id,
                event_type="game_event.projection",
                payload={
                    "operation_id": operation_id,
                    "user_id": user_id,
                    "event_key": "pet_travel_claim",
                    "amount": 1,
                    "occurred_at": occurred_at,
                    "stat_increments": {
                        "宠物游历次数": 1,
                        "宠物游历时长": int(travel.get("duration_hours", 0) or 0),
                    },
                    "meta": {
                        "source": "pet",
                        "action": "travel_claim",
                        "trace_id": operation_id,
                        "stone_delta": stone,
                        "exp_delta": exp,
                        "item_delta": [
                            {"id": item_id, "amount": amount}
                            for item_id, _, _, amount in rewards
                        ],
                        "detail": {
                            "duration_hours": int(travel.get("duration_hours", 0) or 0),
                            "scene": travel.get("scene"),
                            "pet_uid": travel.get("pet_uid"),
                        },
                    },
                },
            )
            return PetTravelClaimResult("applied", stone, exp, tuple((row[0], row[3]) for row in rewards), effects_event_id)


# A receipt row exists but its payload cannot be interpreted.  The hatch did
# happen, so the command must report an explicit failure and must not charge
# again; it is deliberately not part of ``PetHatchResult.succeeded``.
RECEIPT_CORRUPT = "receipt_unreadable"
# A brand-new pet's uid already belongs to another row for this player (legacy
# import or a racing writer).  The insert is rolled back and nothing is charged.
UID_CONFLICT = "uid_conflict"


@dataclass(frozen=True)
class PetHatchResult:
    status: str
    cost: int = 0
    pets: tuple = ()
    updated_meta: tuple = ()
    bag_limit: int = 0
    reason: str = ""

    @property
    def succeeded(self) -> bool:
        return self.status in {"applied", "duplicate"}


class PetHatchSqlRepository:
    def __init__(self, game_database: str | Path, player_database: str | Path, *, clock: Any | None = None) -> None:
        self.game_database = str(game_database)
        self.player_database = str(player_database)
        self.clock = clock or SystemClock()

    def get_result(self, operation_id: str) -> PetHatchResult | None:
        operation_id = str(operation_id).strip()
        if not operation_id:
            return None
        with DatabaseUnitOfWork(self.game_database) as uow:
            row = uow.query_one("SELECT payload,result_json FROM pet_hatch_operations WHERE operation_id=?", (operation_id,))
        if row is None:
            return None
        try:
            pets, stored_meta, stored_limit = decode_receipt(row["result_json"])
        except ReceiptDecodeError:
            # The receipt exists, so the operation did happen: fail closed instead
            # of replaying "no pets" (which would look like a successful hatch).
            return PetHatchResult(RECEIPT_CORRUPT)
        return PetHatchResult("duplicate", 0, pets, stored_meta, stored_limit)

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
        result_json = encode_receipt(normalized, updated_meta, bag_limit)
        with DatabaseUnitOfWork(self.game_database, immediate=True) as uow:
            uow.attach_database(self.player_database, "player_data")
            if not operations_ready(uow, "pet_hatch_operations", frozenset({"operation_id", "payload", "result_json"})):
                return PetHatchResult(SCHEMA_MISSING)
            old = uow.query_one("SELECT payload,result_json FROM pet_hatch_operations WHERE operation_id=?", (operation_id,))
            if old is not None:
                if str(old["payload"]) != payload:
                    return PetHatchResult("state_changed", reason="operation_conflict")
                try:
                    pets, stored_meta, stored_limit = decode_receipt(old["result_json"])
                except ReceiptDecodeError:
                    # Receipt exists but is unreadable: never fall through to a
                    # second charge, and never pretend the replay was empty.
                    return PetHatchResult(RECEIPT_CORRUPT)
                return PetHatchResult("duplicate", cost, pets, stored_meta, stored_limit or bag_limit)
            user = uow.query_one("SELECT stone FROM user_xiuxian WHERE user_id=?", (user_id,))
            if user is None:
                return PetHatchResult("user_missing")
            if as_int_like(user["stone"] or 0) != expected_stone:
                return PetHatchResult("state_changed", reason="stone_changed")
            if expected_stone < cost:
                return PetHatchResult("stone_missing")
            meta = uow.query_one("SELECT active_uid,egg_pity_count,egg_pity_no_mythic_count,travel FROM player_data.player_pet WHERE user_id=?", (user_id,))
            expected = list(expected_meta or []) + [None] * 4
            actual = ["" if meta is None else str(meta["active_uid"] or ""), 0 if meta is None else int(meta["egg_pity_count"] or 0), 0 if meta is None else int(meta["egg_pity_no_mythic_count"] or 0), None if meta is None else self._normalize_travel(meta["travel"])]
            expected[0], expected[3] = str(expected[0] or ""), self._normalize_travel(expected[3])
            if actual[0] != expected[0]:
                return PetHatchResult("state_changed", reason="active_changed")
            if actual[1:3] != expected[1:3]:
                return PetHatchResult("state_changed", reason="pity_changed")
            if actual[3] != expected[3]:
                return PetHatchResult("state_changed", reason="travel_changed")
            owned = uow.query_one("SELECT COUNT(*) AS count FROM player_data.player_pet_item WHERE user_id=?", (user_id,))
            if int(owned["count"]) + len(normalized) > bag_limit:
                return PetHatchResult("inventory_full")
            now = int(self.clock.now().timestamp())
            # A UID is allocated per pet at creation time (``new_pet_uid``), but a
            # pre-existing row can still own the same uid (imported/legacy data or
            # two writers racing).  The UNIQUE(user_id, uid) violation used to
            # escape as an unhandled IntegrityError, so the player saw a bare
            # failure with no explanation.  Roll the item rows back within a
            # savepoint and report a typed status instead: the stone is never
            # charged and the operation row is never written.
            try:
                with uow.savepoint("pet_hatch_items"):
                    for pet, active in normalized:
                        skill = pet.get("skill") or ((pet.get("skills") or [{}])[0])
                        uow.execute("INSERT INTO player_data.player_pet_item(id,user_id,uid,is_active,pet_id,stars,exp,total_exp,skill_id,created_at,updated_at) VALUES(?,?,?,?,?,?,?,?,?,?,?)", (f"{user_id}:{pet['uid']}", user_id, str(pet["uid"]), int(active), str(pet.get("pet_id", "")), int(pet.get("stars", 1)), as_int_like(pet.get("exp", 0)), as_int_like(pet.get("total_exp", 0)), str(skill.get("skill_id", "")) or None, now, now))
            except sqlite3.IntegrityError as exc:
                # Only the ``UNIQUE(user_id, uid)`` clash is a UID conflict; any
                # other integrity failure is a real defect and must still surface.
                if "player_pet_item.user_id" in str(exc):
                    return PetHatchResult(UID_CONFLICT)
                raise
            if uow.execute("UPDATE user_xiuxian SET stone=stone-? WHERE user_id=? AND xiuxian_int_cmp(stone,?)=0", (cost, user_id, expected_stone)).rowcount != 1:
                raise DomainError("stone_write_rejected", "stone row count changed inside hatch transaction")
            if meta is None:
                uow.execute("INSERT INTO player_data.player_pet(user_id,active_uid,egg_pity_count,egg_pity_no_mythic_count,travel) VALUES(?,?,?,?,NULL)", (user_id, updated_meta[0], updated_meta[1], updated_meta[2]))
            elif uow.execute("UPDATE player_data.player_pet SET active_uid=?,egg_pity_count=?,egg_pity_no_mythic_count=? WHERE user_id=?", (*updated_meta, user_id)).rowcount != 1:
                raise DomainError("pet_meta_write_rejected", "pet metadata row count changed inside hatch transaction")
            uow.execute("INSERT INTO pet_hatch_operations(operation_id,payload,result_json) VALUES(?,?,?)", (operation_id, payload, result_json))
            return PetHatchResult("applied", cost, normalized, tuple(updated_meta), bag_limit)


@dataclass(frozen=True)
class PetReleaseResult:
    status: str
    refund: int = 0
    released_uids: tuple[str, ...] = ()
    reason: str = ""

    @property
    def succeeded(self) -> bool:
        return self.status in {"applied", "duplicate"}


class PetReleaseSqlRepository:
    def __init__(self, game_database: str | Path, player_database: str | Path) -> None:
        self.game_database = str(game_database)
        self.player_database = str(player_database)

    def release(self, operation_id: str, user_id: str, uid: str, expected_exp: int, refund_item: int, refund_name: str, refund_type: str, refund: int, max_goods: int, expected_is_active: bool = True) -> PetReleaseResult:
        return self.release_batch(operation_id, user_id, [{"uid": uid, "total_exp": expected_exp, "is_active": int(bool(expected_is_active))}], refund_item, refund_name, refund_type, refund, max_goods, allow_active=True)

    def release_batch(self, operation_id: str, user_id: str, expected_pets: Any, refund_item: int, refund_name: str, refund_type: str, refund: int, max_goods: int, allow_active: bool = False) -> PetReleaseResult:
        operation_id, user_id = str(operation_id).strip(), str(user_id)
        refund_item, refund, max_goods = int(refund_item), int(refund), int(max_goods)
        pets = tuple(sorted((str(pet["uid"]), as_int_like(pet.get("total_exp", 0)), int(pet.get("is_active", 0))) for pet in expected_pets))
        if not operation_id or not pets or len({pet[0] for pet in pets}) != len(pets) or min(refund_item, refund, max_goods) < 0:
            raise ValueError("operation and unique pet snapshots are required")
        payload = json.dumps([user_id, pets, refund_item, str(refund_name), str(refund_type), refund, max_goods, bool(allow_active)], ensure_ascii=True, separators=(",", ":"))
        with DatabaseUnitOfWork(self.game_database, immediate=True) as uow:
            uow.attach_database(self.player_database, "player_data")
            if not operations_ready(uow, "pet_release_operations", RELEASE_COLUMNS):
                return PetReleaseResult(SCHEMA_MISSING)
            old = uow.query_one("SELECT payload,refund,released_uids FROM pet_release_operations WHERE operation_id=?", (operation_id,))
            if old is not None:
                return PetReleaseResult("duplicate" if str(old["payload"]) == payload else "state_changed", int(old["refund"]) if str(old["payload"]) == payload else 0, tuple(json.loads(old["released_uids"])) if str(old["payload"]) == payload else (), reason="" if str(old["payload"]) == payload else "operation_conflict")
            placeholders = ",".join("?" for _ in pets)
            uids = tuple(pet[0] for pet in pets)
            rows = uow.query_all(f"SELECT uid,COALESCE(total_exp,0) AS total_exp,COALESCE(is_active,0) AS is_active FROM player_data.player_pet_item WHERE user_id=? AND uid IN ({placeholders})", (user_id, *uids))
            current = tuple(sorted((str(row["uid"]), as_int_like(row["total_exp"]), int(row["is_active"])) for row in rows))
            if len(current) != len(pets):
                return PetReleaseResult("state_changed", reason="pet_missing")
            if any(a[1] != b[1] for a, b in zip(current, pets)):
                return PetReleaseResult("state_changed", reason="pet_total_exp_changed")
            if current != pets:
                return PetReleaseResult("state_changed", reason="active_changed")
            if not allow_active and any(pet[2] for pet in pets):
                return PetReleaseResult("active_pet")
            active_uids = tuple(pet[0] for pet in pets if pet[2])
            if active_uids:
                meta = uow.query_one("SELECT active_uid FROM player_data.player_pet WHERE user_id=?", (user_id,))
                if meta is None:
                    return PetReleaseResult("state_changed", reason="meta_missing")
                if str(meta["active_uid"] or "") != active_uids[0]:
                    return PetReleaseResult("state_changed", reason="active_changed")
            inventory = uow.query_one("SELECT COALESCE(goods_num,0) AS goods_num FROM back WHERE user_id=? AND goods_id=?", (user_id, refund_item))
            if (int(inventory["goods_num"]) if inventory else 0) + refund > max_goods:
                return PetReleaseResult("inventory_full")
            deleted = uow.execute(f"DELETE FROM player_data.player_pet_item WHERE user_id=? AND uid IN ({placeholders})", (user_id, *uids)).rowcount
            if deleted != len(pets):
                raise DomainError("pet_release_write_rejected", "pet row count changed inside release transaction")
            if active_uids and uow.execute("UPDATE player_data.player_pet SET active_uid=NULL,active=NULL WHERE user_id=? AND active_uid=?", (user_id, active_uids[0])).rowcount != 1:
                raise RuntimeError("active pet metadata changed")
            if refund:
                uow.execute("INSERT INTO back(user_id,goods_id,goods_name,goods_type,goods_num,bind_num) VALUES(?,?,?,?,?,?) ON CONFLICT(user_id,goods_id) DO UPDATE SET goods_name=excluded.goods_name,goods_type=excluded.goods_type,goods_num=back.goods_num+excluded.goods_num,bind_num=COALESCE(back.bind_num,0)+excluded.goods_num", (user_id, refund_item, str(refund_name), str(refund_type), refund, refund))
            uow.execute("INSERT INTO pet_release_operations(operation_id,payload,refund,released_uids) VALUES(?,?,?,?)", (operation_id, payload, refund, json.dumps(uids)))
            return PetReleaseResult("applied", refund, uids)


@dataclass(frozen=True)
class PetFusionBreakthroughResult:
    status: str
    stars: int = 0
    exp: int = 0
    reason: str = ""

    @property
    def succeeded(self) -> bool:
        return self.status in {"applied", "duplicate"}


class PetFusionBreakthroughSqlRepository:
    def __init__(self, database: str | Path) -> None:
        self.database = str(database)

    def breakthrough(self, operation_id: str, user_id: str, expected_main: Any, expected_materials: Any, updated_stars: int, updated_exp: int, skill_offer: Any = None) -> PetFusionBreakthroughResult:
        operation_id, user_id = str(operation_id).strip(), str(user_id)
        main = tuple(expected_main)
        materials = tuple(sorted(tuple(row) for row in expected_materials))
        updated_stars, updated_exp = int(updated_stars), int(updated_exp)
        if not operation_id or len(main) != 7 or not materials or any(len(row) != 7 for row in materials):
            raise ValueError("operation, main snapshot and materials are required")
        payload = json.dumps([user_id, main, materials, updated_stars, updated_exp, skill_offer], ensure_ascii=True, sort_keys=True, separators=(",", ":"))
        with DatabaseUnitOfWork(self.database, immediate=True) as uow:
            if not operations_ready(uow, "pet_fusion_breakthrough_operations", FUSION_COLUMNS):
                return PetFusionBreakthroughResult(SCHEMA_MISSING)
            old = uow.query_one("SELECT payload,stars,exp FROM pet_fusion_breakthrough_operations WHERE operation_id=?", (operation_id,))
            if old is not None:
                return PetFusionBreakthroughResult("duplicate" if str(old["payload"]) == payload else "state_changed", int(old["stars"]) if str(old["payload"]) == payload else 0, as_int_like(old["exp"]) if str(old["payload"]) == payload else 0, reason="" if str(old["payload"]) == payload else "operation_conflict")
            current = uow.query_one("SELECT uid,pet_id,stars,exp,total_exp,COALESCE(skill_id,''),is_active FROM player_pet_item WHERE user_id=? AND uid=?", (user_id, str(main[0])))
            if current is None:
                return PetFusionBreakthroughResult("state_changed", reason="pet_missing")
            if int(current["is_active"]) != 1:
                return PetFusionBreakthroughResult("state_changed", reason="pet_inactive")
            if tuple(current.values()) != main:
                return PetFusionBreakthroughResult("state_changed", reason=_pet_snapshot_reason(tuple(current.values()), main))
            placeholders = ",".join("?" for _ in materials)
            uids = tuple(str(row[0]) for row in materials)
            rows = uow.query_all(f"SELECT uid,pet_id,stars,exp,total_exp,COALESCE(skill_id,''),is_active FROM player_pet_item WHERE user_id=? AND uid IN ({placeholders})", (user_id, *uids))
            if len(rows) != len(materials):
                return PetFusionBreakthroughResult("state_changed", reason="material_missing")
            if any(int(row["is_active"]) for row in rows):
                return PetFusionBreakthroughResult("state_changed", reason="material_active")
            if tuple(sorted(tuple(row.values()) for row in rows)) != materials:
                return PetFusionBreakthroughResult("state_changed", reason="material_changed")
            if uow.execute(f"DELETE FROM player_pet_item WHERE user_id=? AND uid IN ({placeholders})", (user_id, *uids)).rowcount != len(materials):
                raise DomainError("pet_material_write_rejected", "material row count changed inside fusion transaction")
            if uow.execute("UPDATE player_pet_item SET stars=?,exp=? WHERE user_id=? AND uid=? AND stars=? AND xiuxian_int_cmp(exp,?)=0 AND is_active=1", (updated_stars, updated_exp, user_id, str(main[0]), int(main[2]), int(main[3]))).rowcount != 1:
                raise DomainError("pet_fusion_write_rejected", "main pet row count changed inside fusion transaction")
            uow.execute("INSERT INTO pet_fusion_breakthrough_operations(operation_id,payload,stars,exp) VALUES(?,?,?,?)", (operation_id, payload, updated_stars, updated_exp))
            return PetFusionBreakthroughResult("applied", updated_stars, updated_exp)


@dataclass(frozen=True)
class PetSkillRerollResult:
    status: str
    skill_id: str = ""
    reason: str = ""

    @property
    def succeeded(self) -> bool:
        return self.status in {"applied", "duplicate"}


class PetSkillRerollSqlRepository:
    def __init__(self, game_database: str | Path, player_database: str | Path) -> None:
        self.game_database = str(game_database)
        self.player_database = str(player_database)

    def reroll(self, operation_id: str, user_id: str, expected_pet: Any, new_skill_id: str, item_id: int) -> PetSkillRerollResult:
        operation_id, user_id, new_skill_id = str(operation_id).strip(), str(user_id), str(new_skill_id)
        expected_pet, item_id = tuple(expected_pet), int(item_id)
        if not operation_id or len(expected_pet) != 7 or not new_skill_id:
            raise ValueError("operation, pet snapshot and new skill are required")
        payload = json.dumps([user_id, expected_pet, new_skill_id, item_id], ensure_ascii=True, separators=(",", ":"))
        with DatabaseUnitOfWork(self.game_database, immediate=True) as uow:
            uow.attach_database(self.player_database, "player_data")
            if not operations_ready(uow, "pet_skill_reroll_operations", REROLL_COLUMNS):
                return PetSkillRerollResult(SCHEMA_MISSING)
            old = uow.query_one("SELECT payload,skill_id FROM pet_skill_reroll_operations WHERE operation_id=?", (operation_id,))
            if old is not None:
                return PetSkillRerollResult("duplicate" if str(old["payload"]) == payload else "state_changed", str(old["skill_id"]) if str(old["payload"]) == payload else "", reason="" if str(old["payload"]) == payload else "operation_conflict")
            pet = uow.query_one("SELECT uid,pet_id,stars,exp,total_exp,COALESCE(skill_id,'') AS skill_id,is_active FROM player_data.player_pet_item WHERE user_id=? AND uid=?", (user_id, str(expected_pet[0])))
            if pet is None:
                return PetSkillRerollResult("state_changed", reason="pet_missing")
            if tuple(pet.values()) != expected_pet:
                return PetSkillRerollResult("state_changed", reason=_pet_snapshot_reason(tuple(pet.values()), expected_pet))
            item = uow.query_one("SELECT goods_num FROM back WHERE user_id=? AND goods_id=?", (user_id, item_id))
            if item is None or int(item["goods_num"]) < 1:
                return PetSkillRerollResult("item_missing")
            if uow.execute("UPDATE back SET goods_num=goods_num-1 WHERE user_id=? AND goods_id=? AND goods_num>=1", (user_id, item_id)).rowcount != 1:
                raise DomainError("item_write_rejected", "item row count changed inside reroll transaction")
            if uow.execute("UPDATE player_data.player_pet_item SET skill_id=? WHERE user_id=? AND uid=? AND COALESCE(skill_id,'')=?", (new_skill_id, user_id, str(expected_pet[0]), str(expected_pet[5]))).rowcount != 1:
                raise DomainError("pet_skill_write_rejected", "pet row count changed inside reroll transaction")
            uow.execute("INSERT INTO pet_skill_reroll_operations(operation_id,payload,skill_id) VALUES(?,?,?)", (operation_id, payload, new_skill_id))
            return PetSkillRerollResult("applied", new_skill_id)


@dataclass(frozen=True)
class PetSkillReplaceResult:
    status: str
    skill_id: str = ""
    reason: str = ""

    @property
    def succeeded(self) -> bool:
        return self.status in {"applied", "duplicate"}


class PetSkillReplaceSqlRepository:
    def __init__(self, database: str | Path, *, clock: Any | None = None) -> None:
        self.database = str(database)
        self.clock = clock or SystemClock()

    def replace(self, operation_id: str, user_id: str, uid: str, expected_skill_id: str, new_skill_id: str) -> PetSkillReplaceResult:
        operation_id = str(operation_id).strip()
        user_id, uid = str(user_id), str(uid).strip()
        expected_skill_id, new_skill_id = str(expected_skill_id), str(new_skill_id).strip()
        if not operation_id or not user_id or not uid or not new_skill_id:
            raise ValueError("operation, user, pet and replacement skill are required")
        payload = json.dumps(
            [user_id, uid, expected_skill_id, new_skill_id],
            ensure_ascii=True,
            separators=(",", ":"),
        )
        with DatabaseUnitOfWork(self.database, immediate=True) as uow:
            if not operations_ready(uow, "pet_skill_replace_operations", REROLL_COLUMNS):
                return PetSkillReplaceResult(SCHEMA_MISSING)
            old = uow.query_one(
                "SELECT payload,skill_id FROM pet_skill_replace_operations WHERE operation_id=?",
                (operation_id,),
            )
            if old is not None:
                return PetSkillReplaceResult(
                    "duplicate" if str(old["payload"]) == payload else "state_changed",
                    str(old["skill_id"]) if str(old["payload"]) == payload else "",
                    reason="" if str(old["payload"]) == payload else "operation_conflict",
                )
            pet = uow.query_one(
                "SELECT COALESCE(skill_id,'') AS skill_id FROM player_pet_item WHERE user_id=? AND uid=?",
                (user_id, uid),
            )
            if pet is None:
                return PetSkillReplaceResult("state_changed", reason="pet_missing")
            if str(pet["skill_id"] or "") != expected_skill_id:
                return PetSkillReplaceResult("state_changed", reason="skill_changed")
            changed = uow.execute(
                "UPDATE player_pet_item SET skill_id=?,updated_at=? "
                "WHERE user_id=? AND uid=? AND COALESCE(skill_id,'')=?",
                (new_skill_id, int(self.clock.now().timestamp()), user_id, uid, expected_skill_id),
            )
            if changed.rowcount != 1:
                raise DomainError("pet_skill_write_rejected", "pet row count changed inside skill replace transaction")
            uow.execute(
                "INSERT INTO pet_skill_replace_operations(operation_id,payload,skill_id) VALUES(?,?,?)",
                (operation_id, payload, new_skill_id),
            )
            return PetSkillReplaceResult("applied", new_skill_id)


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
        from ...compatibility.legacy_pet_transactions import (
            PetFeedService,
            PetHatchService,
            PetTravelClaimService,
            PetTravelStartService,
        )

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
        from ...compatibility.legacy_pet_transactions import PetActiveSwitchService

        return PetActiveSwitchService(self.player_database).switch(*args, **kwargs)


__all__ = ["PetActiveSwitchResult", "PetActiveSwitchSqlRepository", "PetFeedResult", "PetFeedSqlRepository", "PetTravelStartResult", "PetTravelStartSqlRepository", "PetTravelClaimResult", "PetTravelClaimSqlRepository", "PetHatchResult", "PetHatchSqlRepository", "PetReleaseResult", "PetReleaseSqlRepository", "PetFusionBreakthroughResult", "PetFusionBreakthroughSqlRepository", "PetSkillRerollResult", "PetSkillRerollSqlRepository", "PetSkillReplaceResult", "PetSkillReplaceSqlRepository", "PetRepository", "LegacyPetRepository"]
