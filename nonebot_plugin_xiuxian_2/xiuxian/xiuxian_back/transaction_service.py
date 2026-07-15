from __future__ import annotations

import json
from contextlib import closing
from dataclasses import asdict, dataclass, field
from pathlib import Path
from threading import RLock
from ..xiuxian_utils import db_backend
from typing import Callable
import hashlib
from datetime import datetime
import time

@dataclass(frozen=True)
class PackageReward:
    item_id: int | None
    name: str
    item_type: str | None
    quantity: int

@dataclass(frozen=True)
class PackageOpenResult:
    status: str
    user_id: str
    package_id: int
    quantity: int
    rewards: tuple[PackageReward, ...]

    @property
    def succeeded(self) -> bool:
        return self.status in {"applied", "duplicate"}

class PackageRewardService:
    """Consume packages and grant main-database rewards atomically."""

    def __init__(self, database: str | Path, lock: RLock | None = None) -> None:
        self._database = Path(database)
        self._lock = lock or RLock()

    @staticmethod
    def _ensure_operations(conn) -> None:
        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS package_reward_operations (
                operation_id TEXT PRIMARY KEY,
                user_id TEXT NOT NULL,
                package_id INTEGER NOT NULL,
                quantity INTEGER NOT NULL,
                rewards_json TEXT NOT NULL,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
            """
        )

    @staticmethod
    def _decode_rewards(payload: str) -> tuple[PackageReward, ...]:
        return tuple(PackageReward(**entry) for entry in json.loads(payload))

    def apply(
        self,
        operation_id,
        user_id,
        package_id,
        quantity,
        rewards,
        *,
        max_goods_num,
    ) -> PackageOpenResult:
        operation_id = str(operation_id).strip()
        if not operation_id:
            raise ValueError("operation_id must not be empty")
        user_id = str(user_id)
        package_id = int(package_id)
        quantity = int(quantity)
        max_goods_num = int(max_goods_num)
        if quantity <= 0 or max_goods_num <= 0:
            raise ValueError("quantity and max_goods_num must be positive")
        normalized = tuple(
            reward if isinstance(reward, PackageReward) else PackageReward(
                None if reward[0] is None else int(reward[0]),
                str(reward[1]),
                None if reward[2] is None else str(reward[2]),
                int(reward[3]),
            )
            for reward in rewards
        )
        if not normalized or any(
            reward.quantity == 0
            or (reward.name != "灵石" and reward.quantity < 0)
            for reward in normalized
        ):
            raise ValueError("item rewards must be positive and stone rewards non-zero")
        if any(reward.name != "灵石" and reward.item_id is None for reward in normalized):
            raise ValueError("item rewards require item_id")
        if any(reward.name == "灵石" and reward.item_id is not None for reward in normalized):
            raise ValueError("stone rewards must not contain item_id")

        def result(status, result_quantity=quantity, result_rewards=normalized):
            return PackageOpenResult(
                status, user_id, package_id, int(result_quantity), tuple(result_rewards)
            )

        with self._lock, closing(db_backend.connect(self._database)) as conn:
            try:
                conn.execute("BEGIN IMMEDIATE")
                self._ensure_operations(conn)
                previous = conn.execute(
                    "SELECT user_id, package_id, quantity, rewards_json "
                    "FROM package_reward_operations WHERE operation_id=%s",
                    (operation_id,),
                ).fetchone()
                if previous is not None:
                    conn.rollback()
                    if (
                        str(previous[0]) != user_id
                        or int(previous[1]) != package_id
                        or int(previous[2]) != quantity
                    ):
                        return result("state_changed")
                    return result(
                        "duplicate", previous[2], self._decode_rewards(previous[3])
                    )

                user = conn.execute(
                    "SELECT 1 FROM user_xiuxian WHERE user_id=%s", (user_id,)
                ).fetchone()
                if user is None:
                    conn.rollback()
                    return result("user_missing")
                package = conn.execute(
                    "SELECT goods_num FROM back WHERE user_id=%s AND goods_id=%s",
                    (user_id, package_id),
                ).fetchone()
                if package is None or int(package[0] or 0) < quantity:
                    conn.rollback()
                    return result("item_insufficient")

                columns = set(conn.column_names("back"))
                bind_update = ""
                params: list[object] = [quantity]
                if "bind_num" in columns:
                    bind_update = ", bind_num=MIN(COALESCE(bind_num, 0), goods_num-%s)"
                    params.append(quantity)
                consumed = conn.execute(
                    f"UPDATE back SET goods_num=goods_num-%s{bind_update} "
                    "WHERE user_id=%s AND goods_id=%s AND goods_num>=%s",
                    (*params, user_id, package_id, quantity),
                )
                if consumed.rowcount != 1:
                    conn.rollback()
                    return result("state_changed")

                stone_delta = sum(
                    reward.quantity for reward in normalized if reward.name == "灵石"
                )
                if stone_delta:
                    stone_update = conn.execute(
                        "UPDATE user_xiuxian SET stone=stone+%s "
                        "WHERE user_id=%s AND stone+%s>=0",
                        (stone_delta, user_id, stone_delta),
                    )
                    if stone_update.rowcount != 1:
                        conn.rollback()
                        return result("stone_insufficient")

                item_totals: dict[int, int] = {}
                for reward in normalized:
                    if reward.name != "灵石":
                        item_totals[reward.item_id] = (
                            item_totals.get(reward.item_id, 0) + reward.quantity
                        )
                for item_id, reward_quantity in item_totals.items():
                    current = conn.execute(
                        "SELECT goods_num FROM back WHERE user_id=%s AND goods_id=%s",
                        (user_id, item_id),
                    ).fetchone()
                    current_quantity = int(current[0] or 0) if current else 0
                    if current_quantity + reward_quantity > max_goods_num:
                        conn.rollback()
                        return result("inventory_full")

                for reward in normalized:
                    if reward.name == "灵石":
                        continue
                    conn.execute(
                        "INSERT INTO back (user_id, goods_id, goods_name, goods_type, "
                        "goods_num, bind_num) VALUES (%s, %s, %s, %s, %s, %s) "
                        "ON CONFLICT (user_id, goods_id) DO UPDATE SET "
                        "goods_name=EXCLUDED.goods_name, goods_type=EXCLUDED.goods_type, "
                        "goods_num=MIN(COALESCE(back.goods_num, 0)+EXCLUDED.goods_num, %s), "
                        "bind_num=MIN(COALESCE(back.bind_num, 0)+EXCLUDED.goods_num, "
                        "MIN(COALESCE(back.goods_num, 0)+EXCLUDED.goods_num, %s))",
                        (
                            user_id, reward.item_id, reward.name, reward.item_type,
                            reward.quantity, reward.quantity,
                            max_goods_num, max_goods_num,
                        ),
                    )

                payload = json.dumps(
                    [asdict(reward) for reward in normalized], ensure_ascii=False
                )
                conn.execute(
                    "INSERT INTO package_reward_operations "
                    "(operation_id, user_id, package_id, quantity, rewards_json) "
                    "VALUES (%s, %s, %s, %s, %s)",
                    (operation_id, user_id, package_id, quantity, payload),
                )
                conn.commit()
                return result("applied")
            except Exception:
                conn.rollback()
                raise

@dataclass(frozen=True)
class AccessoryPackageResult:
    status: str
    user_id: str
    package_id: int
    quantity: int
    rewards: tuple[PackageReward, ...]
    accessories: tuple[dict, ...]

    @property
    def succeeded(self) -> bool:
        return self.status in {"applied", "duplicate"}

class AccessoryPackageService:
    """Grant mixed package rewards in one attached-database transaction."""

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
    def _ensure_schema(conn) -> None:
        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS accessory_package_operations (
                operation_id TEXT PRIMARY KEY,
                user_id TEXT NOT NULL,
                package_id INTEGER NOT NULL,
                quantity INTEGER NOT NULL,
                rewards_json TEXT NOT NULL,
                accessories_json TEXT NOT NULL,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
            """
        )
        conn.execute(
            "CREATE TABLE IF NOT EXISTS player_data.player_accessory "
            "(user_id TEXT PRIMARY KEY, equipped TEXT, bag TEXT)"
        )
        columns = {
            str(row[1])
            for row in conn.execute(
                "PRAGMA player_data.table_info(player_accessory)"
            ).fetchall()
        }
        if "equipped" not in columns:
            conn.execute("ALTER TABLE player_data.player_accessory ADD COLUMN equipped TEXT")
        if "bag" not in columns:
            conn.execute("ALTER TABLE player_data.player_accessory ADD COLUMN bag TEXT")

    @staticmethod
    def _decode_rewards(payload: str) -> tuple[PackageReward, ...]:
        return tuple(PackageReward(**entry) for entry in json.loads(payload))

    @staticmethod
    def _load_list(payload) -> list:
        if not payload:
            return []
        value = json.loads(payload) if isinstance(payload, str) else payload
        return value if isinstance(value, list) else []

    @staticmethod
    def _load_equipped(payload) -> dict:
        if not payload:
            return {}
        value = json.loads(payload) if isinstance(payload, str) else payload
        return value if isinstance(value, dict) else {}

    def apply(
        self,
        operation_id,
        user_id,
        package_id,
        quantity,
        rewards,
        accessories,
        *,
        max_goods_num,
        accessory_limit,
    ) -> AccessoryPackageResult:
        operation_id = str(operation_id).strip()
        if not operation_id:
            raise ValueError("operation_id must not be empty")
        user_id = str(user_id)
        package_id = int(package_id)
        quantity = int(quantity)
        max_goods_num = int(max_goods_num)
        accessory_limit = int(accessory_limit)
        normalized_rewards = tuple(rewards)
        normalized_accessories = tuple(dict(item) for item in accessories)
        if quantity <= 0 or max_goods_num <= 0 or accessory_limit <= 0:
            raise ValueError("quantities and limits must be positive")
        if not normalized_accessories:
            raise ValueError("accessories must not be empty")
        if any(not str(item.get("uid", "")).strip() for item in normalized_accessories):
            raise ValueError("accessory uid must not be empty")
        if len({str(item["uid"]) for item in normalized_accessories}) != len(normalized_accessories):
            raise ValueError("accessory uid must be unique")

        def result(status, result_quantity=quantity, result_rewards=normalized_rewards,
                   result_accessories=normalized_accessories):
            return AccessoryPackageResult(
                status, user_id, package_id, int(result_quantity),
                tuple(result_rewards), tuple(result_accessories),
            )

        with self._lock, closing(db_backend.connect(self._game_database)) as conn:
            conn.execute("ATTACH DATABASE %s AS player_data", (str(self._player_database),))
            try:
                conn.execute("BEGIN IMMEDIATE")
                self._ensure_schema(conn)
                previous = conn.execute(
                    "SELECT user_id, package_id, quantity, rewards_json, accessories_json "
                    "FROM accessory_package_operations WHERE operation_id=%s",
                    (operation_id,),
                ).fetchone()
                if previous is not None:
                    conn.rollback()
                    if (
                        str(previous[0]) != user_id
                        or int(previous[1]) != package_id
                        or int(previous[2]) != quantity
                    ):
                        return result("state_changed")
                    return result(
                        "duplicate", previous[2], self._decode_rewards(previous[3]),
                        tuple(json.loads(previous[4])),
                    )

                if conn.execute(
                    "SELECT 1 FROM user_xiuxian WHERE user_id=%s", (user_id,)
                ).fetchone() is None:
                    conn.rollback()
                    return result("user_missing")
                package = conn.execute(
                    "SELECT goods_num FROM back WHERE user_id=%s AND goods_id=%s",
                    (user_id, package_id),
                ).fetchone()
                if package is None or int(package[0] or 0) < quantity:
                    conn.rollback()
                    return result("item_insufficient")

                accessory_row = conn.execute(
                    "SELECT equipped, bag FROM player_data.player_accessory WHERE user_id=%s",
                    (user_id,),
                ).fetchone()
                equipped = self._load_equipped(accessory_row[0]) if accessory_row else {}
                bag = self._load_list(accessory_row[1]) if accessory_row else []
                equipped_count = sum(1 for item in equipped.values() if item)
                if len(bag) + equipped_count + len(normalized_accessories) > accessory_limit:
                    conn.rollback()
                    return result("accessory_full")

                item_totals: dict[int, int] = {}
                stone_delta = 0
                for reward in normalized_rewards:
                    if reward.name == "灵石":
                        stone_delta += reward.quantity
                    else:
                        item_totals[reward.item_id] = item_totals.get(reward.item_id, 0) + reward.quantity
                for item_id, reward_quantity in item_totals.items():
                    current = conn.execute(
                        "SELECT goods_num FROM back WHERE user_id=%s AND goods_id=%s",
                        (user_id, item_id),
                    ).fetchone()
                    if (int(current[0] or 0) if current else 0) + reward_quantity > max_goods_num:
                        conn.rollback()
                        return result("inventory_full")

                columns = set(conn.column_names("back"))
                bind_update = ""
                params: list[object] = [quantity]
                if "bind_num" in columns:
                    bind_update = ", bind_num=MIN(COALESCE(bind_num, 0), goods_num-%s)"
                    params.append(quantity)
                consumed = conn.execute(
                    f"UPDATE back SET goods_num=goods_num-%s{bind_update} "
                    "WHERE user_id=%s AND goods_id=%s AND goods_num>=%s",
                    (*params, user_id, package_id, quantity),
                )
                if consumed.rowcount != 1:
                    conn.rollback()
                    return result("state_changed")

                if stone_delta:
                    updated = conn.execute(
                        "UPDATE user_xiuxian SET stone=stone+%s "
                        "WHERE user_id=%s AND stone+%s>=0",
                        (stone_delta, user_id, stone_delta),
                    )
                    if updated.rowcount != 1:
                        conn.rollback()
                        return result("stone_insufficient")

                for reward in normalized_rewards:
                    if reward.name == "灵石":
                        continue
                    conn.execute(
                        "INSERT INTO back (user_id, goods_id, goods_name, goods_type, "
                        "goods_num, bind_num) VALUES (%s, %s, %s, %s, %s, %s) "
                        "ON CONFLICT (user_id, goods_id) DO UPDATE SET "
                        "goods_name=EXCLUDED.goods_name, goods_type=EXCLUDED.goods_type, "
                        "goods_num=COALESCE(back.goods_num, 0)+EXCLUDED.goods_num, "
                        "bind_num=MIN(COALESCE(back.bind_num, 0)+EXCLUDED.goods_num, "
                        "COALESCE(back.goods_num, 0)+EXCLUDED.goods_num)",
                        (user_id, reward.item_id, reward.name, reward.item_type,
                         reward.quantity, reward.quantity),
                    )

                bag.extend(normalized_accessories)
                equipped_json = json.dumps(equipped, ensure_ascii=False)
                bag_json = json.dumps(bag, ensure_ascii=False)
                conn.execute(
                    "INSERT INTO player_data.player_accessory (user_id, equipped, bag) "
                    "VALUES (%s, %s, %s) ON CONFLICT (user_id) DO UPDATE SET "
                    "equipped=EXCLUDED.equipped, bag=EXCLUDED.bag",
                    (user_id, equipped_json, bag_json),
                )
                rewards_json = json.dumps(
                    [reward.__dict__ for reward in normalized_rewards], ensure_ascii=False
                )
                accessories_json = json.dumps(normalized_accessories, ensure_ascii=False)
                conn.execute(
                    "INSERT INTO accessory_package_operations "
                    "(operation_id, user_id, package_id, quantity, rewards_json, accessories_json) "
                    "VALUES (%s, %s, %s, %s, %s, %s)",
                    (operation_id, user_id, package_id, quantity, rewards_json, accessories_json),
                )
                conn.commit()
                return result("applied")
            except Exception:
                conn.rollback()
                raise
            finally:
                conn.execute("DETACH DATABASE player_data")

@dataclass(frozen=True)
class AccessoryTransactionResult:
    status: str
    action: str
    user_id: str
    affected: int = 0
    stone_delta: int = 0
    accessory: dict | None = None
    details: dict | None = None

    @property
    def succeeded(self) -> bool:
        return self.status in {"applied", "duplicate"}

class AccessoryTransactionService:
    """Apply accessory and wash-stone changes in one attached DB transaction."""

    _SLOTS = ("手镯", "戒指", "手环", "项链")

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
    def _ensure_schema(conn) -> None:
        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS accessory_transaction_operations (
                operation_id TEXT PRIMARY KEY,
                action TEXT NOT NULL,
                payload TEXT NOT NULL,
                result_json TEXT NOT NULL,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
            """
        )
        conn.execute(
            "CREATE TABLE IF NOT EXISTS player_data.player_accessory "
            "(user_id TEXT PRIMARY KEY, equipped TEXT, bag TEXT)"
        )
        columns = {
            str(row[1])
            for row in conn.execute(
                "PRAGMA player_data.table_info(player_accessory)"
            ).fetchall()
        }
        for field_name in ("equipped", "bag", "preset_1", "preset_2", "preset_3"):
            if field_name not in columns:
                conn.execute(
                    "ALTER TABLE player_data.player_accessory "
                    f"ADD COLUMN {field_name} TEXT"
                )

    @staticmethod
    def _json(value) -> str:
        return json.dumps(value, ensure_ascii=False, sort_keys=True, separators=(",", ":"))

    @staticmethod
    def _load_list(value) -> list:
        if not value:
            return []
        decoded = json.loads(value) if isinstance(value, str) else value
        return decoded if isinstance(decoded, list) else []

    @staticmethod
    def _load_dict(value) -> dict:
        if not value:
            return {}
        decoded = json.loads(value) if isinstance(value, str) else value
        return decoded if isinstance(decoded, dict) else {}

    @staticmethod
    def _find(equipped: dict, bag: list, uid: str):
        for index, item in enumerate(bag):
            if str(item.get("uid", "")) == uid:
                return "bag", index, item
        for slot, item in equipped.items():
            if item and str(item.get("uid", "")) == uid:
                return "equipped", slot, item
        return None, None, None

    @staticmethod
    def _upgrade_signature(accessory: dict) -> tuple[int, str, str, int]:
        return (
            int(accessory.get("item_id", 0)),
            str(accessory.get("part", "")),
            str(accessory.get("set_type", "")),
            int(accessory.get("quality", 1)),
        )

    @classmethod
    def _normalize_equipped(cls, equipped) -> dict:
        equipped = equipped if isinstance(equipped, dict) else {}
        return {slot: equipped.get(slot) for slot in cls._SLOTS}

    @classmethod
    def _normalize_preset(cls, preset) -> dict:
        preset = preset if isinstance(preset, dict) else {}
        normalized = {}
        for slot in cls._SLOTS:
            value = preset.get(slot)
            normalized[slot] = None if value is None or value == "" else str(value)
        return normalized

    @staticmethod
    def _result_from_json(status: str, payload: str):
        value = json.loads(payload)
        return AccessoryTransactionResult(
            status=status,
            action=str(value["action"]),
            user_id=str(value["user_id"]),
            affected=int(value.get("affected", 0)),
            stone_delta=int(value.get("stone_delta", 0)),
            accessory=value.get("accessory"),
            details=value.get("details"),
        )

    def replay(self, operation_id, action) -> AccessoryTransactionResult | None:
        operation_id = str(operation_id).strip()
        if not operation_id:
            raise ValueError("operation_id must not be empty")
        with self._lock, closing(db_backend.connect(self._game_database)) as conn:
            if not conn.table_exists("accessory_transaction_operations"):
                return None
            row = conn.execute(
                "SELECT action, result_json FROM accessory_transaction_operations "
                "WHERE operation_id=%s",
                (operation_id,),
            ).fetchone()
            if row is None or str(row[0]) != str(action):
                return None
            return self._result_from_json("duplicate", str(row[1]))

    def _run(self, operation_id, action, payload, apply: Callable):
        operation_id = str(operation_id).strip()
        if not operation_id:
            raise ValueError("operation_id must not be empty")
        payload_json = self._json(payload)

        with self._lock, closing(db_backend.connect(self._game_database)) as conn:
            conn.execute("ATTACH DATABASE %s AS player_data", (str(self._player_database),))
            try:
                conn.execute("BEGIN IMMEDIATE")
                self._ensure_schema(conn)
                previous = conn.execute(
                    "SELECT action, payload, result_json "
                    "FROM accessory_transaction_operations WHERE operation_id=%s",
                    (operation_id,),
                ).fetchone()
                if previous is not None:
                    conn.rollback()
                    if str(previous[0]) != action or str(previous[1]) != payload_json:
                        return AccessoryTransactionResult(
                            "state_changed", action, str(payload.get("user_id", ""))
                        )
                    return self._result_from_json("duplicate", str(previous[2]))

                result = apply(conn)
                if result.status != "applied":
                    conn.rollback()
                    return result
                result_json = self._json(
                    {
                        "action": result.action,
                        "user_id": result.user_id,
                        "affected": result.affected,
                        "stone_delta": result.stone_delta,
                        "accessory": result.accessory,
                        "details": result.details,
                    }
                )
                conn.execute(
                    "INSERT INTO accessory_transaction_operations "
                    "(operation_id, action, payload, result_json) VALUES (%s, %s, %s, %s)",
                    (operation_id, action, payload_json, result_json),
                )
                conn.commit()
                return result
            except Exception:
                conn.rollback()
                raise
            finally:
                conn.execute("DETACH DATABASE player_data")

    def _load_accessories(self, conn, user_id: str):
        row = conn.execute(
            "SELECT equipped, bag FROM player_data.player_accessory WHERE user_id=%s",
            (user_id,),
        ).fetchone()
        if row is None:
            return {}, []
        return self._load_dict(row[0]), self._load_list(row[1])

    def _load_preset(self, conn, user_id: str, preset_idx: int) -> dict:
        row = conn.execute(
            f"SELECT preset_{preset_idx} FROM player_data.player_accessory "
            "WHERE user_id=%s",
            (user_id,),
        ).fetchone()
        return self._normalize_preset(self._load_dict(row[0]) if row else {})

    @staticmethod
    def _save_accessories(conn, user_id: str, equipped: dict, bag: list) -> None:
        conn.execute(
            "INSERT INTO player_data.player_accessory (user_id, equipped, bag) "
            "VALUES (%s, %s, %s) ON CONFLICT (user_id) DO UPDATE SET "
            "equipped=EXCLUDED.equipped, bag=EXCLUDED.bag",
            (
                user_id,
                json.dumps(equipped, ensure_ascii=False),
                json.dumps(bag, ensure_ascii=False),
            ),
        )

    @staticmethod
    def _save_preset(conn, user_id: str, preset_idx: int, preset: dict) -> None:
        value = json.dumps(preset, ensure_ascii=False)
        conn.execute(
            f"INSERT INTO player_data.player_accessory (user_id, preset_{preset_idx}) "
            f"VALUES (%s, %s) ON CONFLICT (user_id) DO UPDATE SET "
            f"preset_{preset_idx}=EXCLUDED.preset_{preset_idx}",
            (user_id, value),
        )

    @staticmethod
    def _stone_quantity(conn, user_id: str, stone_id: int) -> int:
        row = conn.execute(
            "SELECT goods_num FROM back WHERE user_id=%s AND goods_id=%s",
            (user_id, stone_id),
        ).fetchone()
        return int(row[0] or 0) if row else 0

    @staticmethod
    def _add_stones(conn, user_id, stone_id, stone_name, amount, max_goods_num):
        current = AccessoryTransactionService._stone_quantity(conn, user_id, stone_id)
        if current + amount > max_goods_num:
            return False
        conn.execute(
            "INSERT INTO back (user_id, goods_id, goods_name, goods_type, goods_num, bind_num) "
            "VALUES (%s, %s, %s, %s, %s, %s) "
            "ON CONFLICT (user_id, goods_id) DO UPDATE SET "
            "goods_name=EXCLUDED.goods_name, goods_type=EXCLUDED.goods_type, "
            "goods_num=COALESCE(back.goods_num, 0)+EXCLUDED.goods_num, "
            "bind_num=MIN(COALESCE(back.bind_num, 0)+EXCLUDED.goods_num, "
            "COALESCE(back.goods_num, 0)+EXCLUDED.goods_num)",
            (user_id, stone_id, stone_name, "特殊道具", amount, amount),
        )
        return True

    def wash(
        self,
        operation_id,
        user_id,
        uid,
        expected_accessory,
        expected_stones,
        stone_id,
        stone_cost,
        reroll: Callable[[dict], dict],
    ) -> AccessoryTransactionResult:
        user_id = str(user_id)
        uid = str(uid)
        expected_stones = int(expected_stones)
        stone_id = int(stone_id)
        stone_cost = int(stone_cost)
        payload = {
            "user_id": user_id,
            "uid": uid,
            "expected_accessory": expected_accessory,
            "expected_stones": expected_stones,
            "stone_id": stone_id,
            "stone_cost": stone_cost,
        }

        def apply(conn):
            equipped, bag = self._load_accessories(conn, user_id)
            where, key, current = self._find(equipped, bag, uid)
            if current is None:
                return AccessoryTransactionResult("accessory_missing", "wash", user_id)
            if self._json(current) != self._json(expected_accessory):
                return AccessoryTransactionResult("state_changed", "wash", user_id)
            if self._stone_quantity(conn, user_id, stone_id) != expected_stones:
                return AccessoryTransactionResult("state_changed", "wash", user_id)
            consumed = conn.execute(
                "UPDATE back SET goods_num=goods_num-%s, "
                "bind_num=MIN(COALESCE(bind_num, 0), goods_num-%s) "
                "WHERE user_id=%s AND goods_id=%s AND goods_num>=%s",
                (stone_cost, stone_cost, user_id, stone_id, stone_cost),
            )
            if consumed.rowcount != 1:
                return AccessoryTransactionResult("item_insufficient", "wash", user_id)

            updated = reroll(dict(current))
            if where == "bag":
                bag[key] = updated
            else:
                equipped[key] = updated
            self._save_accessories(conn, user_id, equipped, bag)
            return AccessoryTransactionResult(
                "applied", "wash", user_id, 1, -stone_cost, updated
            )

        return self._run(operation_id, "wash", payload, apply)

    def set_affix_locks(
        self,
        operation_id,
        action,
        user_id,
        uid,
        expected_accessory,
        locked_indexes,
    ) -> AccessoryTransactionResult:
        action = str(action)
        if action not in {"lock", "unlock"}:
            raise ValueError("action must be lock or unlock")
        user_id = str(user_id)
        uid = str(uid)
        locked_indexes = sorted({int(index) for index in locked_indexes})
        payload = {
            "user_id": user_id,
            "uid": uid,
            "expected_accessory": expected_accessory,
            "locked_indexes": locked_indexes,
        }

        def apply(conn):
            equipped, bag = self._load_accessories(conn, user_id)
            where, key, current = self._find(equipped, bag, uid)
            if current is None:
                return AccessoryTransactionResult(
                    "accessory_missing", action, user_id
                )
            if self._json(current) != self._json(expected_accessory):
                return AccessoryTransactionResult("state_changed", action, user_id)

            updated = json.loads(json.dumps(current, ensure_ascii=False))
            if locked_indexes:
                updated["locked_affixes"] = locked_indexes
            else:
                updated.pop("locked_affixes", None)
            if where == "bag":
                bag[key] = updated
            else:
                equipped[key] = updated
            self._save_accessories(conn, user_id, equipped, bag)
            return AccessoryTransactionResult(
                "applied", action, user_id, 1, 0, updated
            )

        return self._run(operation_id, action, payload, apply)

    def decompose(
        self,
        operation_id,
        user_id,
        uid,
        expected_accessory,
        stone_id,
        stone_name,
        stone_gain,
        max_goods_num,
    ) -> AccessoryTransactionResult:
        user_id = str(user_id)
        uid = str(uid)
        stone_id = int(stone_id)
        stone_gain = int(stone_gain)
        max_goods_num = int(max_goods_num)
        payload = {
            "user_id": user_id,
            "uid": uid,
            "expected_accessory": expected_accessory,
            "stone_id": stone_id,
            "stone_gain": stone_gain,
            "max_goods_num": max_goods_num,
        }

        def apply(conn):
            equipped, bag = self._load_accessories(conn, user_id)
            where, key, current = self._find(equipped, bag, uid)
            if current is None or where != "bag":
                return AccessoryTransactionResult("accessory_missing", "decompose", user_id)
            if self._json(current) != self._json(expected_accessory):
                return AccessoryTransactionResult("state_changed", "decompose", user_id)
            if not self._add_stones(
                conn, user_id, stone_id, stone_name, stone_gain, max_goods_num
            ):
                return AccessoryTransactionResult("inventory_full", "decompose", user_id)
            del bag[key]
            self._save_accessories(conn, user_id, equipped, bag)
            return AccessoryTransactionResult(
                "applied", "decompose", user_id, 1, stone_gain, current
            )

        return self._run(operation_id, "decompose", payload, apply)

    def upgrade(
        self,
        operation_id,
        user_id,
        part,
        expected_equipped,
        expected_bag,
        material_uids,
        upgraded_accessory,
    ) -> AccessoryTransactionResult:
        user_id = str(user_id)
        part = str(part)
        raw_material_uids = tuple(str(uid).strip() for uid in material_uids)
        if (
            not part
            or not raw_material_uids
            or any(not uid for uid in raw_material_uids)
            or len(set(raw_material_uids)) != len(raw_material_uids)
        ):
            raise ValueError("part and unique material uids are required")
        material_uids = raw_material_uids
        upgraded_accessory = json.loads(self._json(upgraded_accessory))
        payload = {
            "user_id": user_id,
            "part": part,
            "expected_equipped": expected_equipped,
            "expected_bag": expected_bag,
            "material_uids": material_uids,
            "upgraded_accessory": upgraded_accessory,
        }

        def apply(conn):
            equipped, bag = self._load_accessories(conn, user_id)
            if (
                self._json(equipped) != self._json(expected_equipped)
                or self._json(bag) != self._json(expected_bag)
            ):
                return AccessoryTransactionResult(
                    "state_changed", "upgrade", user_id
                )

            current = equipped.get(part)
            if not isinstance(current, dict):
                return AccessoryTransactionResult(
                    "accessory_missing", "upgrade", user_id
                )
            old_quality = int(current.get("quality", 1))
            if old_quality >= 5:
                return AccessoryTransactionResult(
                    "max_quality", "upgrade", user_id
                )
            required = 1 if old_quality <= 1 else old_quality - 1
            if len(material_uids) != required:
                return AccessoryTransactionResult(
                    "material_mismatch", "upgrade", user_id
                )

            material_indexes = []
            current_signature = self._upgrade_signature(current)
            for uid in material_uids:
                matches = [
                    (index, item)
                    for index, item in enumerate(bag)
                    if str(item.get("uid", "")) == uid
                ]
                if len(matches) != 1:
                    return AccessoryTransactionResult(
                        "material_missing", "upgrade", user_id
                    )
                index, material = matches[0]
                if self._upgrade_signature(material) != current_signature:
                    return AccessoryTransactionResult(
                        "material_mismatch", "upgrade", user_id
                    )
                material_indexes.append(index)

            updated = dict(upgraded_accessory)
            if (
                str(updated.get("uid", "")) != str(current.get("uid", ""))
                or int(updated.get("quality", 0)) != old_quality + 1
                or int(updated.get("wash_count", -1)) != 0
            ):
                return AccessoryTransactionResult(
                    "invalid_plan", "upgrade", user_id
                )
            current_fixed = dict(current)
            updated_fixed = dict(updated)
            for field_name in (
                "quality",
                "wash_count",
                "affixes",
                "locked_affixes",
            ):
                current_fixed.pop(field_name, None)
                updated_fixed.pop(field_name, None)
            if self._json(current_fixed) != self._json(updated_fixed):
                return AccessoryTransactionResult(
                    "invalid_plan", "upgrade", user_id
                )

            for index in sorted(material_indexes, reverse=True):
                del bag[index]
            equipped[part] = updated
            self._save_accessories(conn, user_id, equipped, bag)
            return AccessoryTransactionResult(
                "applied", "upgrade", user_id, required, 0, updated
            )

        return self._run(operation_id, "upgrade", payload, apply)

    @staticmethod
    def _validate_preset_index(preset_idx) -> int:
        preset_idx = int(preset_idx)
        if preset_idx not in {1, 2, 3}:
            raise ValueError("preset index must be 1, 2, or 3")
        return preset_idx

    def save_preset(
        self,
        operation_id,
        user_id,
        preset_idx,
        expected_equipped,
        expected_preset,
    ) -> AccessoryTransactionResult:
        user_id = str(user_id)
        preset_idx = self._validate_preset_index(preset_idx)
        expected_equipped = self._normalize_equipped(expected_equipped)
        expected_preset = self._normalize_preset(expected_preset)
        payload = {
            "user_id": user_id,
            "preset_idx": preset_idx,
            "expected_equipped": expected_equipped,
            "expected_preset": expected_preset,
        }

        def apply(conn):
            equipped, _ = self._load_accessories(conn, user_id)
            equipped = self._normalize_equipped(equipped)
            current_preset = self._load_preset(conn, user_id, preset_idx)
            if (
                self._json(equipped) != self._json(expected_equipped)
                or self._json(current_preset) != self._json(expected_preset)
            ):
                return AccessoryTransactionResult(
                    "state_changed", "save_preset", user_id
                )

            preset = {
                slot: (
                    str(equipped[slot].get("uid"))
                    if isinstance(equipped[slot], dict)
                    and equipped[slot].get("uid") is not None
                    and equipped[slot].get("uid") != ""
                    else None
                )
                for slot in self._SLOTS
            }
            self._save_preset(conn, user_id, preset_idx, preset)
            return AccessoryTransactionResult(
                "applied",
                "save_preset",
                user_id,
                details={
                    "preset_idx": preset_idx,
                    "preset": preset,
                    "had_old": any(current_preset.values()),
                },
            )

        return self._run(operation_id, "save_preset", payload, apply)

    def quick_equip_preset(
        self,
        operation_id,
        user_id,
        preset_idx,
        expected_equipped,
        expected_bag,
        expected_preset,
    ) -> AccessoryTransactionResult:
        user_id = str(user_id)
        preset_idx = self._validate_preset_index(preset_idx)
        expected_equipped = self._normalize_equipped(expected_equipped)
        expected_preset = self._normalize_preset(expected_preset)
        payload = {
            "user_id": user_id,
            "preset_idx": preset_idx,
            "expected_equipped": expected_equipped,
            "expected_bag": expected_bag,
            "expected_preset": expected_preset,
        }

        def apply(conn):
            equipped, bag = self._load_accessories(conn, user_id)
            equipped = self._normalize_equipped(equipped)
            preset = self._load_preset(conn, user_id, preset_idx)
            if (
                self._json(equipped) != self._json(expected_equipped)
                or self._json(bag) != self._json(expected_bag)
                or self._json(preset) != self._json(expected_preset)
            ):
                return AccessoryTransactionResult(
                    "state_changed", "quick_equip_preset", user_id
                )
            if not any(preset.values()):
                return AccessoryTransactionResult(
                    "preset_empty", "quick_equip_preset", user_id
                )

            equipped_results = []
            skipped_results = []
            missing_results = []
            for slot in self._SLOTS:
                uid = preset.get(slot)
                if not uid:
                    continue

                current = equipped.get(slot)
                if current and str(current.get("uid", "")) == uid:
                    skipped_results.append(
                        {"slot": slot, "reason": "already_equipped"}
                    )
                    continue

                where, key, target = self._find(equipped, bag, uid)
                if target is None:
                    preset[slot] = None
                    missing_results.append({"slot": slot})
                    continue
                if str(target.get("part", "")) != slot:
                    skipped_results.append(
                        {
                            "slot": slot,
                            "reason": "part_mismatch",
                            "name": str(target.get("name", "未知饰品")),
                        }
                    )
                    continue

                old = equipped.get(slot)
                if where == "bag":
                    del bag[key]
                else:
                    equipped[key] = None
                if old:
                    bag.append(old)
                equipped[slot] = target
                equipped_results.append(
                    {"slot": slot, "name": str(target.get("name", "未知饰品"))}
                )

            self._save_accessories(conn, user_id, equipped, bag)
            self._save_preset(conn, user_id, preset_idx, preset)
            return AccessoryTransactionResult(
                "applied",
                "quick_equip_preset",
                user_id,
                affected=len(equipped_results),
                details={
                    "preset_idx": preset_idx,
                    "preset": preset,
                    "equipped": equipped_results,
                    "skipped": skipped_results,
                    "missing": missing_results,
                },
            )

        return self._run(operation_id, "quick_equip_preset", payload, apply)

    def batch_decompose(
        self,
        operation_id,
        user_id,
        expected_bag,
        selected_uids,
        stone_id,
        stone_name,
        total_gain,
        max_goods_num,
    ) -> AccessoryTransactionResult:
        user_id = str(user_id)
        selected_uids = tuple(str(uid) for uid in selected_uids)
        stone_id = int(stone_id)
        total_gain = int(total_gain)
        max_goods_num = int(max_goods_num)
        payload = {
            "user_id": user_id,
            "expected_bag": expected_bag,
            "selected_uids": selected_uids,
            "stone_id": stone_id,
            "total_gain": total_gain,
            "max_goods_num": max_goods_num,
        }

        def apply(conn):
            equipped, bag = self._load_accessories(conn, user_id)
            if self._json(bag) != self._json(expected_bag):
                return AccessoryTransactionResult("state_changed", "batch_decompose", user_id)
            selected = set(selected_uids)
            hit = [item for item in bag if str(item.get("uid", "")) in selected]
            if len(hit) != len(selected):
                return AccessoryTransactionResult("accessory_missing", "batch_decompose", user_id)
            if not self._add_stones(
                conn, user_id, stone_id, stone_name, total_gain, max_goods_num
            ):
                return AccessoryTransactionResult("inventory_full", "batch_decompose", user_id)
            bag = [item for item in bag if str(item.get("uid", "")) not in selected]
            self._save_accessories(conn, user_id, equipped, bag)
            return AccessoryTransactionResult(
                "applied", "batch_decompose", user_id, len(hit), total_gain
            )

        return self._run(operation_id, "batch_decompose", payload, apply)

@dataclass(frozen=True)
class AlchemyResult:
    status: str
    user_id: str
    reward_stone: int
    consumed: tuple[tuple[int, int], ...]

    @property
    def succeeded(self) -> bool:
        return self.status in {"applied", "duplicate"}

class AlchemyService:
    """Atomically consume an alchemy batch and grant its stone reward."""

    def __init__(self, database: str | Path, lock: RLock | None = None) -> None:
        self._database = Path(database)
        self._lock = lock or RLock()

    @staticmethod
    def _ensure_operations(conn) -> None:
        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS alchemy_operations (
                operation_id TEXT PRIMARY KEY,
                payload TEXT NOT NULL,
                user_id TEXT NOT NULL,
                reward_stone INTEGER NOT NULL,
                consumed TEXT NOT NULL,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
            """
        )

    def apply(self, operation_id, user_id, reward_stone, consume_items) -> AlchemyResult:
        operation_id = str(operation_id).strip()
        if not operation_id:
            raise ValueError("operation_id must not be empty")
        user_id = str(user_id)
        reward_stone = int(reward_stone)
        normalized: dict[int, int] = {}
        for goods_id, quantity in consume_items:
            goods_id = int(goods_id)
            quantity = int(quantity)
            if quantity <= 0:
                raise ValueError("alchemy quantity must be positive")
            normalized[goods_id] = normalized.get(goods_id, 0) + quantity
        consumed = tuple(sorted(normalized.items()))
        if reward_stone <= 0 or not consumed:
            raise ValueError("alchemy reward and consumed items must be positive")
        payload = json.dumps(
            [user_id, reward_stone, consumed], separators=(",", ":"), ensure_ascii=True
        )

        def result(status: str) -> AlchemyResult:
            return AlchemyResult(status, user_id, reward_stone, consumed)

        with self._lock, closing(db_backend.connect(self._database)) as conn:
            try:
                conn.execute("BEGIN IMMEDIATE")
                self._ensure_operations(conn)
                previous = conn.execute(
                    "SELECT payload FROM alchemy_operations WHERE operation_id=%s",
                    (operation_id,),
                ).fetchone()
                if previous is not None:
                    conn.rollback()
                    return result("duplicate" if str(previous[0]) == payload else "conflict")

                if conn.execute(
                    "SELECT 1 FROM user_xiuxian WHERE user_id=%s", (user_id,)
                ).fetchone() is None:
                    conn.rollback()
                    return result("user_missing")

                back_columns = set(conn.column_names("back"))
                for goods_id, quantity in consumed:
                    row = conn.execute(
                        "SELECT goods_num, state FROM back WHERE user_id=%s AND goods_id=%s",
                        (user_id, goods_id),
                    ).fetchone()
                    if row is None or int(row[0] or 0) - int(row[1] or 0) < quantity:
                        conn.rollback()
                        return result("item_insufficient")

                for goods_id, quantity in consumed:
                    updates = ["goods_num=goods_num-%s"]
                    params: list[object] = [quantity]
                    if "bind_num" in back_columns:
                        updates.append("bind_num=MIN(COALESCE(bind_num, 0), goods_num-%s)")
                        params.append(quantity)
                    if "update_time" in back_columns:
                        updates.append("update_time=CURRENT_TIMESTAMP")
                    if "action_time" in back_columns:
                        updates.append("action_time=CURRENT_TIMESTAMP")
                    updated = conn.execute(
                        f"UPDATE back SET {', '.join(updates)} "
                        "WHERE user_id=%s AND goods_id=%s "
                        "AND COALESCE(goods_num, 0)-COALESCE(state, 0)>=%s",
                        (*params, user_id, goods_id, quantity),
                    )
                    if updated.rowcount != 1:
                        conn.rollback()
                        return result("item_insufficient")

                granted = conn.execute(
                    "UPDATE user_xiuxian SET stone=stone+%s WHERE user_id=%s",
                    (reward_stone, user_id),
                )
                if granted.rowcount != 1:
                    conn.rollback()
                    return result("user_missing")
                conn.execute(
                    "INSERT INTO alchemy_operations "
                    "(operation_id,payload,user_id,reward_stone,consumed) "
                    "VALUES (%s,%s,%s,%s,%s)",
                    (operation_id, payload, user_id, reward_stone, json.dumps(consumed)),
                )
                conn.commit()
                return result("applied")
            except Exception:
                conn.rollback()
                raise

@dataclass(frozen=True)
class BackpackRepairResult:
    status: str
    operation_id: str
    total: int = 0
    completed: int = 0
    quantity_fixed: int = 0
    bind_fixed: int = 0
    name_fixed: int = 0
    equipment_fixed: int = 0
    missing_definitions: int = 0
    details: tuple[dict, ...] = ()

    @property
    def succeeded(self) -> bool:
        return self.status in {"applied", "duplicate"}

    @property
    def done(self) -> bool:
        return self.succeeded and self.completed >= self.total

class BackpackRepairService:
    """Repair all backpack rows through a resumable database task."""

    _TASK_COLUMNS = (
        "operation_id",
        "payload",
        "catalog_json",
        "max_goods_num",
        "targets_json",
        "next_index",
        "total",
        "quantity_fixed",
        "bind_fixed",
        "name_fixed",
        "equipment_fixed",
        "missing_definitions",
        "details_json",
        "status",
    )

    def __init__(self, database: str | Path, lock: RLock | None = None) -> None:
        self._database = Path(database)
        self._lock = lock or RLock()

    @staticmethod
    def _ensure_schema(conn) -> None:
        conn.execute(
            "CREATE TABLE IF NOT EXISTS backpack_repair_tasks("
            "operation_id TEXT PRIMARY KEY,payload TEXT NOT NULL,"
            "catalog_json TEXT NOT NULL,max_goods_num INTEGER NOT NULL,"
            "targets_json TEXT NOT NULL,"
            "next_index INTEGER NOT NULL DEFAULT 0,total INTEGER NOT NULL,"
            "quantity_fixed INTEGER NOT NULL DEFAULT 0,"
            "bind_fixed INTEGER NOT NULL DEFAULT 0,"
            "name_fixed INTEGER NOT NULL DEFAULT 0,"
            "equipment_fixed INTEGER NOT NULL DEFAULT 0,"
            "missing_definitions INTEGER NOT NULL DEFAULT 0,"
            "details_json TEXT NOT NULL DEFAULT '[]',"
            "status TEXT NOT NULL DEFAULT 'running',"
            "created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,"
            "updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP)"
        )

    @staticmethod
    def _json(value) -> str:
        return json.dumps(
            value, ensure_ascii=False, sort_keys=True, separators=(",", ":")
        )

    @classmethod
    def _normalize_catalog(cls, catalog) -> dict[str, str]:
        if not isinstance(catalog, dict):
            raise ValueError("item catalog must be a mapping")
        return {
            str(item_id): str(item_name)
            for item_id, item_name in catalog.items()
            if str(item_id).strip()
            and item_name is not None
            and str(item_name).strip()
        }

    @classmethod
    def _payload(cls, catalog: dict[str, str], max_goods_num: int) -> str:
        request = cls._json(
            {"catalog": catalog, "max_goods_num": int(max_goods_num)}
        )
        return hashlib.sha256(request.encode("utf-8")).hexdigest()

    @classmethod
    def _fetch_task(cls, conn, operation_id: str):
        row = conn.execute(
            "SELECT " + ",".join(cls._TASK_COLUMNS) +
            " FROM backpack_repair_tasks WHERE operation_id=%s",
            (operation_id,),
        ).fetchone()
        if row is None:
            return None
        return dict(zip(cls._TASK_COLUMNS, row))

    @staticmethod
    def _collect_targets(conn) -> list[dict[str, str]]:
        targets = {}
        if conn.table_exists("user_xiuxian"):
            for user_id, user_name in conn.execute(
                "SELECT user_id,user_name FROM user_xiuxian ORDER BY user_id"
            ).fetchall():
                targets[str(user_id)] = str(user_name or user_id)
        if conn.table_exists("back"):
            for row in conn.execute(
                "SELECT DISTINCT user_id FROM back ORDER BY user_id"
            ).fetchall():
                user_id = str(row[0])
                targets.setdefault(user_id, user_id)
        return [
            {"user_id": user_id, "user_name": targets[user_id]}
            for user_id in sorted(targets)
        ]

    @classmethod
    def _result(cls, task, status: str | None = None) -> BackpackRepairResult:
        details = json.loads(str(task["details_json"]) or "[]")
        return BackpackRepairResult(
            status or str(task["status"]),
            str(task["operation_id"]),
            int(task["total"]),
            int(task["next_index"]),
            int(task["quantity_fixed"]),
            int(task["bind_fixed"]),
            int(task["name_fixed"]),
            int(task["equipment_fixed"]),
            int(task["missing_definitions"]),
            tuple(details),
        )

    @staticmethod
    def _append_detail(details: list[dict], detail: dict) -> None:
        if len(details) < 20:
            details.append(detail)

    @staticmethod
    def _as_int(value) -> int:
        try:
            return int(value or 0)
        except (TypeError, ValueError):
            return 0

    def _repair_equipment(
        self,
        conn,
        user_id: str,
        user_name: str,
        item_id: int,
        item_type: str,
        catalog: dict[str, str],
        back_columns: set[str],
        details: list[dict],
    ) -> tuple[int, int]:
        item_name = catalog.get(str(item_id))
        if not item_name:
            self._append_detail(
                details,
                {
                    "kind": "missing_definition",
                    "user_id": user_id,
                    "user_name": user_name,
                    "item_id": item_id,
                    "item_type": item_type,
                },
            )
            return 0, 1

        row = conn.execute(
            "SELECT goods_num,bind_num,state,goods_name FROM back "
            "WHERE user_id=%s AND goods_id=%s",
            (user_id, item_id),
        ).fetchone()
        now = datetime.now()
        fixes = []
        if row is None:
            values = {
                "user_id": user_id,
                "goods_id": item_id,
                "goods_name": item_name,
                "goods_type": "装备",
                "goods_num": 1,
                "bind_num": 1,
                "state": 1,
                "create_time": now,
                "update_time": now,
                "action_time": now,
            }
            names = [name for name in values if name in back_columns]
            conn.execute(
                "INSERT INTO back(" + ",".join(names) + ") VALUES(" +
                ",".join(["%s"] * len(names)) + ")",
                tuple(values[name] for name in names),
            )
            fixes.extend(("quantity", "state", "name"))
        else:
            goods_num = self._as_int(row[0])
            bind_num = self._as_int(row[1])
            state = self._as_int(row[2])
            old_name = str(row[3] or "")
            updates = []
            params = []
            if goods_num <= 0:
                updates.append("goods_num=%s")
                params.append(1)
                if "bind_num" in back_columns:
                    updates.append("bind_num=%s")
                    params.append(1)
                fixes.append("quantity")
            elif bind_num > goods_num and "bind_num" in back_columns:
                updates.append("bind_num=%s")
                params.append(goods_num)
            if state != 1:
                updates.append("state=%s")
                params.append(1)
                fixes.append("state")
            if old_name != item_name:
                updates.append("goods_name=%s")
                params.append(item_name)
                fixes.append("name")
            if updates:
                if "update_time" in back_columns:
                    updates.append("update_time=%s")
                    params.append(now)
                if "action_time" in back_columns:
                    updates.append("action_time=%s")
                    params.append(now)
                params.extend((user_id, item_id))
                conn.execute(
                    "UPDATE back SET " + ",".join(updates) +
                    " WHERE user_id=%s AND goods_id=%s",
                    tuple(params),
                )

        if fixes:
            self._append_detail(
                details,
                {
                    "kind": "equipment",
                    "user_id": user_id,
                    "user_name": user_name,
                    "item_id": item_id,
                    "item_name": item_name,
                    "item_type": item_type,
                    "fixes": fixes,
                },
            )
            return 1, 0
        return 0, 0

    def _repair_user(
        self,
        conn,
        target: dict,
        catalog: dict[str, str],
        max_goods_num: int,
        details: list[dict],
    ) -> tuple[int, int, int, int, int]:
        user_id = str(target["user_id"])
        user_name = str(target["user_name"])
        quantity_fixed = bind_fixed = name_fixed = 0
        equipment_fixed = missing_definitions = 0
        back_columns = set(conn.column_names("back"))
        rows = conn.execute(
            "SELECT goods_id,goods_num,bind_num,goods_name FROM back "
            "WHERE user_id=%s ORDER BY goods_id",
            (user_id,),
        ).fetchall()
        for item_id, goods_num_raw, bind_num_raw, goods_name_raw in rows:
            item_id = int(item_id)
            goods_num = self._as_int(goods_num_raw)
            bind_num = self._as_int(bind_num_raw)
            goods_name = str(goods_name_raw or "")
            new_goods_num = min(max(goods_num, 0), max_goods_num)
            new_bind_num = min(max(bind_num, 0), new_goods_num)
            new_name = catalog.get(str(item_id), goods_name)
            updates = []
            params = []
            if new_goods_num != goods_num:
                updates.append("goods_num=%s")
                params.append(new_goods_num)
                quantity_fixed += 1
                self._append_detail(
                    details,
                    {
                        "kind": "quantity",
                        "user_id": user_id,
                        "item_id": item_id,
                        "before": goods_num,
                        "after": new_goods_num,
                    },
                )
            if new_bind_num != bind_num:
                updates.append("bind_num=%s")
                params.append(new_bind_num)
                bind_fixed += 1
                self._append_detail(
                    details,
                    {
                        "kind": "bind_quantity",
                        "user_id": user_id,
                        "item_id": item_id,
                        "before": bind_num,
                        "after": new_bind_num,
                    },
                )
            if new_name != goods_name:
                updates.append("goods_name=%s")
                params.append(new_name)
                name_fixed += 1
                self._append_detail(
                    details,
                    {
                        "kind": "name",
                        "user_id": user_id,
                        "item_id": item_id,
                        "before": goods_name,
                        "after": new_name,
                    },
                )
            if updates:
                if "update_time" in back_columns:
                    updates.append("update_time=%s")
                    params.append(datetime.now())
                params.extend((user_id, item_id))
                conn.execute(
                    "UPDATE back SET " + ",".join(updates) +
                    " WHERE user_id=%s AND goods_id=%s",
                    tuple(params),
                )

        if conn.table_exists("BuffInfo"):
            buff_columns = set(conn.column_names("BuffInfo"))
            selected = [
                column
                for column in ("faqi_buff", "armor_buff")
                if column in buff_columns
            ]
            if selected:
                buff_row = conn.execute(
                    "SELECT " + ",".join(selected) +
                    " FROM BuffInfo WHERE user_id=%s",
                    (user_id,),
                ).fetchone()
                if buff_row:
                    item_types = {"faqi_buff": "法器", "armor_buff": "防具"}
                    seen = set()
                    for column, value in zip(selected, buff_row):
                        item_id = self._as_int(value)
                        if item_id <= 0 or item_id in seen:
                            continue
                        seen.add(item_id)
                        fixed, missing = self._repair_equipment(
                            conn,
                            user_id,
                            user_name,
                            item_id,
                            item_types[column],
                            catalog,
                            back_columns,
                            details,
                        )
                        equipment_fixed += fixed
                        missing_definitions += missing

        return (
            quantity_fixed,
            bind_fixed,
            name_fixed,
            equipment_fixed,
            missing_definitions,
        )

    def run(
        self,
        operation_id,
        catalog=None,
        max_goods_num=None,
        *,
        batch_size=100,
    ) -> BackpackRepairResult:
        operation_id = str(operation_id).strip()
        batch_size = int(batch_size)
        if not operation_id or batch_size <= 0:
            raise ValueError("operation id and positive batch size are required")
        normalized_catalog = None
        request_payload = None
        if catalog is not None or max_goods_num is not None:
            if catalog is None or max_goods_num is None or int(max_goods_num) <= 0:
                raise ValueError("catalog and inventory limit must be provided together")
            normalized_catalog = self._normalize_catalog(catalog)
            max_goods_num = int(max_goods_num)
            request_payload = self._payload(normalized_catalog, max_goods_num)

        with self._lock, closing(db_backend.connect(self._database)) as conn:
            try:
                conn.execute("BEGIN IMMEDIATE")
                self._ensure_schema(conn)
                created = False
                resumed_active = False
                task = self._fetch_task(conn, operation_id)
                if task is None:
                    active = conn.execute(
                        "SELECT operation_id FROM backpack_repair_tasks "
                        "WHERE status='running' ORDER BY created_at LIMIT 1"
                    ).fetchone()
                    if active is not None:
                        operation_id = str(active[0])
                        task = self._fetch_task(conn, operation_id)
                        resumed_active = True
                    else:
                        if normalized_catalog is None or request_payload is None:
                            conn.rollback()
                            return BackpackRepairResult("task_missing", operation_id)
                        targets = self._collect_targets(conn)
                        conn.execute(
                            "INSERT INTO backpack_repair_tasks("
                            "operation_id,payload,catalog_json,max_goods_num,"
                            "targets_json,total,status) VALUES(%s,%s,%s,%s,%s,%s,%s)",
                            (
                                operation_id,
                                request_payload,
                                self._json(normalized_catalog),
                                max_goods_num,
                                self._json(targets),
                                len(targets),
                                "running" if targets else "completed",
                            ),
                        )
                        task = self._fetch_task(conn, operation_id)
                        created = True

                if (
                    request_payload is not None
                    and not resumed_active
                    and str(task["payload"]) != request_payload
                ):
                    conn.rollback()
                    return BackpackRepairResult("operation_conflict", operation_id)
                if str(task["status"]) == "completed":
                    if created:
                        conn.commit()
                        return self._result(task, "applied")
                    conn.rollback()
                    return self._result(task, "duplicate")

                catalog_snapshot = json.loads(str(task["catalog_json"]))
                max_goods_num = int(task["max_goods_num"])

                targets = json.loads(str(task["targets_json"]))
                details = json.loads(str(task["details_json"]) or "[]")
                start = int(task["next_index"])
                end = min(start + batch_size, int(task["total"]))
                counters = [
                    int(task["quantity_fixed"]),
                    int(task["bind_fixed"]),
                    int(task["name_fixed"]),
                    int(task["equipment_fixed"]),
                    int(task["missing_definitions"]),
                ]
                for target in targets[start:end]:
                    changes = self._repair_user(
                        conn,
                        target,
                        catalog_snapshot,
                        max_goods_num,
                        details,
                    )
                    counters = [left + right for left, right in zip(counters, changes)]

                status = "completed" if end >= int(task["total"]) else "running"
                conn.execute(
                    "UPDATE backpack_repair_tasks SET next_index=%s,"
                    "quantity_fixed=%s,bind_fixed=%s,name_fixed=%s,"
                    "equipment_fixed=%s,missing_definitions=%s,details_json=%s,"
                    "status=%s,updated_at=CURRENT_TIMESTAMP WHERE operation_id=%s",
                    (
                        end,
                        *counters,
                        self._json(details),
                        status,
                        operation_id,
                    ),
                )
                conn.commit()
                return self._result(self._fetch_task(conn, operation_id), "applied")
            except Exception:
                conn.rollback()
                raise

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

@dataclass(frozen=True)
class BlessedFlagReplaceResult:
    status: str
    user_id: str
    item_id: int
    previous_level: int = 0
    current_level: int = 0
    herb_speed: int = 0
    quantity: int = 0

    @property
    def succeeded(self) -> bool:
        return self.status in {"applied", "duplicate"}

class BlessedFlagReplaceService:
    """Replace a blessed-spot flag in one cross-database transaction."""

    def __init__(self, game_database: str | Path, player_database: str | Path,
                 lock: RLock | None = None) -> None:
        self._game_database = Path(game_database)
        self._player_database = Path(player_database)
        self._lock = lock or RLock()

    @staticmethod
    def _payload(values) -> str:
        return json.dumps(values, ensure_ascii=True, separators=(",", ":"))

    @staticmethod
    def _ensure_schema(conn) -> None:
        conn.execute(
            "CREATE TABLE IF NOT EXISTS blessed_flag_replace_operations ("
            "operation_id TEXT PRIMARY KEY,payload TEXT NOT NULL,result_json TEXT NOT NULL,"
            "created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP)"
        )

    @staticmethod
    def _saved(status: str, data: dict) -> BlessedFlagReplaceResult:
        return BlessedFlagReplaceResult(
            status, str(data["user_id"]), int(data["item_id"]),
            int(data["previous_level"]), int(data["current_level"]),
            int(data["herb_speed"]), int(data["quantity"]),
        )

    def replace(self, operation_id, user_id, item_id, target_level, herb_speed, *,
                expected_level, expected_herb_speed,
                expected_quantity) -> BlessedFlagReplaceResult:
        operation_id = str(operation_id).strip()
        if not operation_id:
            raise ValueError("operation_id must not be empty")
        user_id = str(user_id)
        item_id, target_level, herb_speed = int(item_id), int(target_level), int(herb_speed)
        expected_level = int(expected_level)
        expected_herb_speed = int(expected_herb_speed)
        expected_quantity = int(expected_quantity)
        if item_id <= 0 or min(target_level, herb_speed, expected_quantity) < 0:
            raise ValueError("item, level, speed and quantity must be valid")
        payload = self._payload([
            user_id, item_id, target_level, herb_speed, expected_level,
            expected_herb_speed, expected_quantity,
        ])

        def result(status, previous_level=expected_level, quantity=0):
            return BlessedFlagReplaceResult(
                status, user_id, item_id, int(previous_level), target_level,
                herb_speed, int(quantity),
            )

        with self._lock, closing(db_backend.connect(self._game_database)) as conn:
            conn.execute("ATTACH DATABASE %s AS player_data", (str(self._player_database),))
            try:
                conn.execute("BEGIN IMMEDIATE")
                self._ensure_schema(conn)
                previous = conn.execute(
                    "SELECT payload,result_json FROM blessed_flag_replace_operations "
                    "WHERE operation_id=%s", (operation_id,),
                ).fetchone()
                if previous is not None:
                    conn.rollback()
                    if str(previous[0]) != payload:
                        return result("state_changed")
                    return self._saved("duplicate", json.loads(str(previous[1])))

                user = conn.execute(
                    "SELECT COALESCE(blessed_spot_flag,0) FROM user_xiuxian WHERE user_id=%s",
                    (user_id,),
                ).fetchone()
                if user is None:
                    conn.rollback()
                    return result("user_missing")
                if int(user[0] or 0) == 0:
                    conn.rollback()
                    return result("blessed_spot_missing")
                buff = conn.execute(
                    "SELECT COALESCE(blessed_spot,0) FROM BuffInfo WHERE user_id=%s",
                    (user_id,),
                ).fetchone()
                if buff is None:
                    conn.rollback()
                    return result("buff_missing")
                current_level = int(buff[0] or 0)
                if current_level != expected_level:
                    conn.rollback()
                    return result("state_changed", current_level)
                if target_level < current_level:
                    conn.rollback()
                    return result("downgrade", current_level)
                if target_level == current_level:
                    conn.rollback()
                    return result("same_level", current_level)

                inventory = conn.execute(
                    "SELECT COALESCE(goods_num,0) FROM back WHERE user_id=%s AND goods_id=%s",
                    (user_id, item_id),
                ).fetchone()
                current_quantity = int(inventory[0] or 0) if inventory else 0
                if current_quantity != expected_quantity:
                    conn.rollback()
                    return result("state_changed", current_level)
                if current_quantity < 1:
                    conn.rollback()
                    return result("item_missing", current_level)
                try:
                    speed_row = conn.execute(
                        f"SELECT {db_backend.quote_ident('药材速度')} FROM "
                        "player_data.mix_elixir_info WHERE user_id=%s", (user_id,),
                    ).fetchone()
                except db_backend.OperationalError:
                    conn.rollback()
                    return result("mix_elixir_missing", current_level)
                if speed_row is None:
                    conn.rollback()
                    return result("mix_elixir_missing", current_level)
                if int(speed_row[0] or 0) != expected_herb_speed:
                    conn.rollback()
                    return result("state_changed", current_level)

                columns = set(conn.column_names("back"))
                updates = ["goods_num=goods_num-1"]
                if "bind_num" in columns:
                    updates.append(
                        "bind_num=CASE WHEN goods_num-1=0 THEN 0 "
                        "WHEN COALESCE(bind_num,0)>=1 THEN COALESCE(bind_num,0)-1 "
                        "ELSE MIN(COALESCE(bind_num,0),goods_num-1) END"
                    )
                if "update_time" in columns:
                    updates.append("update_time=CURRENT_TIMESTAMP")
                if "action_time" in columns:
                    updates.append("action_time=CURRENT_TIMESTAMP")
                consumed = conn.execute(
                    f"UPDATE back SET {', '.join(updates)} WHERE user_id=%s "
                    "AND goods_id=%s AND goods_num=%s AND goods_num>=1",
                    (user_id, item_id, expected_quantity),
                )
                level_updated = conn.execute(
                    "UPDATE BuffInfo SET blessed_spot=%s WHERE user_id=%s "
                    "AND COALESCE(blessed_spot,0)=%s",
                    (target_level, user_id, expected_level),
                )
                speed_column = db_backend.quote_ident("药材速度")
                speed_updated = conn.execute(
                    f"UPDATE player_data.mix_elixir_info SET {speed_column}=%s "
                    f"WHERE user_id=%s AND CAST(COALESCE({speed_column},0) AS INTEGER)=%s",
                    (str(herb_speed), user_id, expected_herb_speed),
                )
                if any(change.rowcount != 1 for change in
                       (consumed, level_updated, speed_updated)):
                    conn.rollback()
                    return result("state_changed", current_level)
                saved = {
                    "user_id": user_id, "item_id": item_id,
                    "previous_level": current_level, "current_level": target_level,
                    "herb_speed": herb_speed, "quantity": 1,
                }
                conn.execute(
                    "INSERT INTO blessed_flag_replace_operations "
                    "(operation_id,payload,result_json) VALUES (%s,%s,%s)",
                    (operation_id, payload, json.dumps(saved, ensure_ascii=True)),
                )
                conn.commit()
                return self._saved("applied", saved)
            except Exception:
                conn.rollback()
                raise
            finally:
                conn.execute("DETACH DATABASE player_data")

@dataclass(frozen=True)
class BreakthroughRateItemUse:
    status: str
    user_id: str
    item_id: int
    quantity: int
    rate_gain: int

    @property
    def succeeded(self) -> bool:
        return self.status in {"applied", "duplicate"}

class BreakthroughRateItemService:
    """Consume an elixir and increase breakthrough rate atomically."""

    def __init__(self, database: str | Path, lock: RLock | None = None) -> None:
        self._database = Path(database)
        self._lock = lock or RLock()

    @staticmethod
    def _ensure_operations(conn) -> None:
        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS breakthrough_rate_item_operations (
                operation_id TEXT PRIMARY KEY,
                user_id TEXT NOT NULL,
                item_id INTEGER NOT NULL,
                quantity INTEGER NOT NULL,
                rate_gain INTEGER NOT NULL,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
            """
        )

    def apply(
        self,
        operation_id,
        user_id,
        item_id,
        quantity,
        rate_gain,
    ) -> BreakthroughRateItemUse:
        operation_id = str(operation_id).strip()
        if not operation_id:
            raise ValueError("operation_id must not be empty")
        user_id = str(user_id)
        item_id = int(item_id)
        quantity = int(quantity)
        rate_gain = int(rate_gain)
        if quantity <= 0 or rate_gain < 0:
            raise ValueError("quantity must be positive and rate gain non-negative")

        def result(status: str, values=None) -> BreakthroughRateItemUse:
            values = values or (quantity, rate_gain)
            return BreakthroughRateItemUse(
                status, user_id, item_id, int(values[0]), int(values[1])
            )

        with self._lock, closing(db_backend.connect(self._database)) as conn:
            try:
                conn.execute("BEGIN IMMEDIATE")
                self._ensure_operations(conn)
                previous = conn.execute(
                    "SELECT quantity, rate_gain FROM breakthrough_rate_item_operations "
                    "WHERE operation_id=%s",
                    (operation_id,),
                ).fetchone()
                if previous is not None:
                    conn.rollback()
                    return result("duplicate", previous)

                if conn.execute(
                    "SELECT 1 FROM user_xiuxian WHERE user_id=%s", (user_id,)
                ).fetchone() is None:
                    conn.rollback()
                    return result("user_missing")
                item = conn.execute(
                    "SELECT goods_num FROM back WHERE user_id=%s AND goods_id=%s",
                    (user_id, item_id),
                ).fetchone()
                if item is None or int(item[0] or 0) < quantity:
                    conn.rollback()
                    return result("item_insufficient")

                columns = set(conn.column_names("back"))
                updates = ["goods_num=goods_num-%s"]
                params: list[object] = [quantity]
                if "day_num" in columns:
                    updates.append("day_num=COALESCE(day_num, 0)+%s")
                    params.append(quantity)
                if "all_num" in columns:
                    updates.append("all_num=COALESCE(all_num, 0)+%s")
                    params.append(quantity)
                if "bind_num" in columns:
                    updates.append(
                        "bind_num=CASE WHEN goods_num-%s=0 THEN 0 "
                        "WHEN COALESCE(bind_num, 0)>=%s "
                        "THEN COALESCE(bind_num, 0)-%s "
                        "ELSE MIN(COALESCE(bind_num, 0), goods_num-%s) END"
                    )
                    params.extend((quantity, quantity, quantity, quantity))
                consumed = conn.execute(
                    f"UPDATE back SET {', '.join(updates)} "
                    "WHERE user_id=%s AND goods_id=%s AND goods_num>=%s",
                    (*params, user_id, item_id, quantity),
                )
                updated = conn.execute(
                    "UPDATE user_xiuxian SET level_up_rate="
                    "COALESCE(level_up_rate, 0)+%s WHERE user_id=%s",
                    (rate_gain, user_id),
                )
                if consumed.rowcount != 1 or updated.rowcount != 1:
                    conn.rollback()
                    return result("state_changed")

                conn.execute(
                    "INSERT INTO breakthrough_rate_item_operations "
                    "(operation_id, user_id, item_id, quantity, rate_gain) "
                    "VALUES (%s, %s, %s, %s, %s)",
                    (operation_id, user_id, item_id, quantity, rate_gain),
                )
                conn.commit()
                return result("applied")
            except Exception:
                conn.rollback()
                raise

@dataclass(frozen=True)
class CultivationItemUse:
    status: str
    user_id: str
    item_id: int
    quantity: int
    exp_gain: int

    @property
    def succeeded(self) -> bool:
        return self.status in {"applied", "duplicate"}

class CultivationItemService:
    """Consume cultivation items and update character attributes atomically."""

    def __init__(self, database: str | Path, lock: RLock | None = None) -> None:
        self._database = Path(database)
        self._lock = lock or RLock()

    @staticmethod
    def _ensure_operations(conn) -> None:
        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS cultivation_item_operations (
                operation_id TEXT PRIMARY KEY,
                user_id TEXT NOT NULL,
                item_id INTEGER NOT NULL,
                quantity INTEGER NOT NULL,
                exp_gain INTEGER NOT NULL,
                hp_gain INTEGER NOT NULL,
                mp_gain INTEGER NOT NULL,
                atk_gain INTEGER NOT NULL,
                power_multiplier REAL NOT NULL,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
            """
        )

    def apply(
        self,
        operation_id,
        user_id,
        item_id,
        quantity,
        exp_gain,
        *,
        hp_gain,
        mp_gain,
        atk_gain,
        power_multiplier,
        track_usage=False,
    ) -> CultivationItemUse:
        operation_id = str(operation_id).strip()
        if not operation_id:
            raise ValueError("operation_id must not be empty")
        user_id = str(user_id)
        item_id = int(item_id)
        quantity = int(quantity)
        exp_gain = int(exp_gain)
        hp_gain = int(hp_gain)
        mp_gain = int(mp_gain)
        atk_gain = int(atk_gain)
        power_multiplier = float(power_multiplier)
        track_usage = bool(track_usage)
        if quantity <= 0 or min(exp_gain, hp_gain, mp_gain, atk_gain) < 0:
            raise ValueError("quantity and gains must be non-negative")
        if power_multiplier < 0:
            raise ValueError("power_multiplier must be non-negative")

        def result(status: str, result_quantity=quantity, result_exp=exp_gain):
            return CultivationItemUse(
                status, user_id, item_id, int(result_quantity), int(result_exp)
            )

        with self._lock, closing(db_backend.connect(self._database)) as conn:
            try:
                conn.execute("BEGIN IMMEDIATE")
                self._ensure_operations(conn)
                previous = conn.execute(
                    "SELECT quantity, exp_gain FROM cultivation_item_operations "
                    "WHERE operation_id=%s",
                    (operation_id,),
                ).fetchone()
                if previous is not None:
                    conn.rollback()
                    return result("duplicate", previous[0], previous[1])

                if conn.execute(
                    "SELECT 1 FROM user_xiuxian WHERE user_id=%s", (user_id,)
                ).fetchone() is None:
                    conn.rollback()
                    return result("user_missing")
                item = conn.execute(
                    "SELECT goods_num FROM back WHERE user_id=%s AND goods_id=%s",
                    (user_id, item_id),
                ).fetchone()
                if item is None or int(item[0] or 0) < quantity:
                    conn.rollback()
                    return result("item_insufficient")

                back_columns = set(conn.column_names("back"))
                updates = ["goods_num=goods_num-%s"]
                params: list[object] = [quantity]
                if track_usage and "day_num" in back_columns:
                    updates.append("day_num=COALESCE(day_num, 0)+%s")
                    params.append(quantity)
                if track_usage and "all_num" in back_columns:
                    updates.append("all_num=COALESCE(all_num, 0)+%s")
                    params.append(quantity)
                if "bind_num" in back_columns:
                    updates.append(
                        "bind_num=CASE WHEN goods_num-%s=0 THEN 0 "
                        "WHEN COALESCE(bind_num, 0)>=%s THEN COALESCE(bind_num, 0)-%s "
                        "ELSE MIN(COALESCE(bind_num, 0), goods_num-%s) END"
                    )
                    params.extend((quantity, quantity, quantity, quantity))
                consumed = conn.execute(
                    f"UPDATE back SET {', '.join(updates)} "
                    "WHERE user_id=%s AND goods_id=%s AND goods_num>=%s",
                    (*params, user_id, item_id, quantity),
                )
                updated = conn.execute(
                    """
                    UPDATE user_xiuxian
                    SET exp=COALESCE(exp, 0)+%s,
                        hp=COALESCE(hp, 0)+%s,
                        mp=COALESCE(mp, 0)+%s,
                        atk=COALESCE(atk, 0)+%s,
                        power=ROUND((COALESCE(exp, 0)+%s)*%s, 0)
                    WHERE user_id=%s
                    """,
                    (
                        exp_gain,
                        hp_gain,
                        mp_gain,
                        atk_gain,
                        exp_gain,
                        power_multiplier,
                        user_id,
                    ),
                )
                if consumed.rowcount != 1 or updated.rowcount != 1:
                    conn.rollback()
                    return result("state_changed")

                conn.execute(
                    """
                    INSERT INTO cultivation_item_operations (
                        operation_id, user_id, item_id, quantity, exp_gain,
                        hp_gain, mp_gain, atk_gain, power_multiplier
                    ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
                    """,
                    (
                        operation_id,
                        user_id,
                        item_id,
                        quantity,
                        exp_gain,
                        hp_gain,
                        mp_gain,
                        atk_gain,
                        power_multiplier,
                    ),
                )
                conn.commit()
                return result("applied")
            except Exception:
                conn.rollback()
                raise

@dataclass(frozen=True)
class EquipmentChange:
    status: str
    user_id: str
    goods_id: int
    previous_id: int = 0

    @property
    def succeeded(self) -> bool:
        return self.status in {"equipped", "unequipped", "duplicate"}

class EquipmentService:
    SLOT_COLUMNS = {"法器": "faqi_buff", "防具": "armor_buff"}

    def __init__(self, database: str | Path, lock: RLock | None = None) -> None:
        self._database = Path(database)
        self._lock = lock or RLock()

    @staticmethod
    def _ensure_operations(conn) -> None:
        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS equipment_operations (
                operation_id TEXT PRIMARY KEY,
                user_id TEXT NOT NULL,
                goods_id INTEGER NOT NULL,
                action TEXT NOT NULL,
                previous_id INTEGER NOT NULL DEFAULT 0,
                payload TEXT NOT NULL DEFAULT '',
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
            """
        )

        if "payload" not in conn.column_names("equipment_operations"):
            conn.execute("ALTER TABLE equipment_operations ADD COLUMN payload TEXT NOT NULL DEFAULT ''")
    def change(self, operation_id, user_id, goods_id, item_type, *, equip: bool):
        column = self.SLOT_COLUMNS.get(str(item_type))
        if column is None:
            return EquipmentChange("unsupported_type", str(user_id), int(goods_id))
        operation_id = str(operation_id).strip()
        if not operation_id:
            raise ValueError("operation_id must not be empty")
        user_id = str(user_id)
        payload = json.dumps([user_id, goods_id, str(item_type), bool(equip)], ensure_ascii=True)
        goods_id = int(goods_id)

        with self._lock, closing(db_backend.connect(self._database)) as conn:
            try:
                conn.execute("BEGIN IMMEDIATE")
                self._ensure_operations(conn)
                row = conn.execute(
                    "SELECT previous_id, payload FROM equipment_operations WHERE operation_id=%s",
                    (operation_id,),
                ).fetchone()
                if row:
                    conn.rollback()
                    if str(row[1] or "") != payload:
                        return EquipmentChange("state_changed", user_id, goods_id, int(row[0]))
                    return EquipmentChange("duplicate", user_id, goods_id, int(row[0]))
                inventory = conn.execute(
                    "SELECT goods_num, state FROM back WHERE user_id=%s AND goods_id=%s",
                    (user_id, goods_id),
                ).fetchone()
                if inventory is None or int(inventory[0] or 0) <= 0:
                    conn.rollback()
                    return EquipmentChange("item_missing", user_id, goods_id)
                buff = conn.execute(
                    f"SELECT {column} FROM BuffInfo WHERE user_id=%s", (user_id,)
                ).fetchone()
                if buff is None:
                    conn.rollback()
                    return EquipmentChange("buff_missing", user_id, goods_id)
                previous_id = int(buff[0] or 0)
                if equip and previous_id == goods_id and int(inventory[1] or 0) == 1:
                    conn.rollback()
                    return EquipmentChange("already_equipped", user_id, goods_id, previous_id)
                if not equip and previous_id != goods_id:
                    conn.rollback()
                    return EquipmentChange("not_equipped", user_id, goods_id, previous_id)

                if previous_id:
                    conn.execute(
                        "UPDATE back SET state=0, update_time=CURRENT_TIMESTAMP, "
                        "action_time=CURRENT_TIMESTAMP WHERE user_id=%s AND goods_id=%s",
                        (user_id, previous_id),
                    )
                target_id = goods_id if equip else 0
                if equip:
                    conn.execute(
                        "UPDATE back SET state=1, update_time=CURRENT_TIMESTAMP, "
                        "action_time=CURRENT_TIMESTAMP WHERE user_id=%s AND goods_id=%s",
                        (user_id, goods_id),
                    )
                conn.execute(
                    f"UPDATE BuffInfo SET {column}=%s WHERE user_id=%s",
                    (target_id, user_id),
                )
                conn.execute(
                    "INSERT INTO equipment_operations (operation_id,user_id,goods_id,action,previous_id,payload,created_at) VALUES (%s, %s, %s, %s, %s, %s, CURRENT_TIMESTAMP)",
                    (operation_id, user_id, goods_id, "equip" if equip else "unequip", previous_id, payload),
                )
                conn.commit()
                return EquipmentChange(
                    "equipped" if equip else "unequipped", user_id, goods_id, previous_id
                )
            except Exception:
                conn.rollback()
                raise

@dataclass(frozen=True)
class LotteryReward:
    item_id: int
    name: str
    item_type: str
    quantity: int

@dataclass(frozen=True)
class LotteryTalismanUse:
    status: str
    user_id: str
    talisman_id: int
    quantity: int
    rewards: tuple[LotteryReward, ...]

    @property
    def succeeded(self) -> bool:
        return self.status in {"applied", "duplicate"}

class LotteryTalismanService:
    """Consume talismans and grant pre-rolled rewards atomically."""

    def __init__(self, database: str | Path, lock: RLock | None = None) -> None:
        self._database = Path(database)
        self._lock = lock or RLock()

    @staticmethod
    def _ensure_operations(conn) -> None:
        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS lottery_talisman_operations (
                operation_id TEXT PRIMARY KEY,
                user_id TEXT NOT NULL,
                talisman_id INTEGER NOT NULL,
                quantity INTEGER NOT NULL,
                rewards_json TEXT NOT NULL,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
            """
        )

    @staticmethod
    def _decode_rewards(payload: str) -> tuple[LotteryReward, ...]:
        return tuple(LotteryReward(**item) for item in json.loads(payload))

    def apply(
        self,
        operation_id,
        user_id,
        talisman_id,
        quantity,
        rewards,
        *,
        max_goods_num,
    ) -> LotteryTalismanUse:
        operation_id = str(operation_id).strip()
        if not operation_id:
            raise ValueError("operation_id must not be empty")
        user_id = str(user_id)
        talisman_id = int(talisman_id)
        quantity = int(quantity)
        max_goods_num = int(max_goods_num)
        if quantity <= 0 or max_goods_num <= 0:
            raise ValueError("quantity and max_goods_num must be positive")
        normalized = tuple(
            reward if isinstance(reward, LotteryReward) else LotteryReward(
                int(reward[0]), str(reward[1]), str(reward[2]), int(reward[3])
            )
            for reward in rewards
        )
        if any(reward.quantity <= 0 for reward in normalized):
            raise ValueError("reward quantities must be positive")

        def result(status: str, result_quantity=quantity, result_rewards=normalized):
            return LotteryTalismanUse(
                status, user_id, talisman_id, int(result_quantity), tuple(result_rewards)
            )

        with self._lock, closing(db_backend.connect(self._database)) as conn:
            try:
                conn.execute("BEGIN IMMEDIATE")
                self._ensure_operations(conn)
                previous = conn.execute(
                    "SELECT quantity, rewards_json FROM lottery_talisman_operations "
                    "WHERE operation_id=%s",
                    (operation_id,),
                ).fetchone()
                if previous is not None:
                    conn.rollback()
                    return result("duplicate", previous[0], self._decode_rewards(previous[1]))
                item = conn.execute(
                    "SELECT goods_num FROM back WHERE user_id=%s AND goods_id=%s",
                    (user_id, talisman_id),
                ).fetchone()
                if item is None or int(item[0] or 0) < quantity:
                    conn.rollback()
                    return result("item_insufficient")

                columns = set(conn.column_names("back"))
                bind_update = ""
                params: list[object] = [quantity]
                if "bind_num" in columns:
                    bind_update = ", bind_num=MIN(COALESCE(bind_num, 0), goods_num-%s)"
                    params.append(quantity)
                consumed = conn.execute(
                    f"UPDATE back SET goods_num=goods_num-%s{bind_update} "
                    "WHERE user_id=%s AND goods_id=%s AND goods_num>=%s",
                    (*params, user_id, talisman_id, quantity),
                )
                if consumed.rowcount != 1:
                    conn.rollback()
                    return result("state_changed")

                for reward in normalized:
                    conn.execute(
                        "INSERT INTO back (user_id, goods_id, goods_name, goods_type, "
                        "goods_num, bind_num) VALUES (%s, %s, %s, %s, %s, %s) "
                        "ON CONFLICT (user_id, goods_id) DO UPDATE SET "
                        "goods_name=EXCLUDED.goods_name, goods_type=EXCLUDED.goods_type, "
                        "goods_num=MIN(COALESCE(back.goods_num, 0)+EXCLUDED.goods_num, %s), "
                        "bind_num=MIN(COALESCE(back.bind_num, 0)+EXCLUDED.goods_num, "
                        "MIN(COALESCE(back.goods_num, 0)+EXCLUDED.goods_num, %s))",
                        (
                            user_id, reward.item_id, reward.name, reward.item_type,
                            reward.quantity, reward.quantity, max_goods_num, max_goods_num,
                        ),
                    )
                payload = json.dumps(
                    [reward.__dict__ for reward in normalized], ensure_ascii=False
                )
                conn.execute(
                    "INSERT INTO lottery_talisman_operations "
                    "(operation_id, user_id, talisman_id, quantity, rewards_json) "
                    "VALUES (%s, %s, %s, %s, %s)",
                    (operation_id, user_id, talisman_id, quantity, payload),
                )
                conn.commit()
                return result("applied")
            except Exception:
                conn.rollback()
                raise

@dataclass(frozen=True)
class PermanentAtkItemUse:
    status: str
    user_id: str
    item_id: int
    quantity: int
    atk_gain: int

    @property
    def succeeded(self) -> bool:
        return self.status in {"applied", "duplicate"}

class PermanentAtkItemService:
    """Consume an elixir and increase permanent attack atomically."""

    def __init__(self, database: str | Path, lock: RLock | None = None) -> None:
        self._database = Path(database)
        self._lock = lock or RLock()

    @staticmethod
    def _ensure_operations(conn) -> None:
        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS permanent_atk_item_operations (
                operation_id TEXT PRIMARY KEY,
                user_id TEXT NOT NULL,
                item_id INTEGER NOT NULL,
                quantity INTEGER NOT NULL,
                atk_gain INTEGER NOT NULL,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
            """
        )

    def apply(self, operation_id, user_id, item_id, quantity, atk_gain):
        operation_id = str(operation_id).strip()
        if not operation_id:
            raise ValueError("operation_id must not be empty")
        user_id = str(user_id)
        item_id = int(item_id)
        quantity = int(quantity)
        atk_gain = int(atk_gain)
        if quantity <= 0 or atk_gain < 0:
            raise ValueError("quantity must be positive and attack gain non-negative")

        def result(status: str, values=None) -> PermanentAtkItemUse:
            values = values or (quantity, atk_gain)
            return PermanentAtkItemUse(
                status, user_id, item_id, int(values[0]), int(values[1])
            )

        with self._lock, closing(db_backend.connect(self._database)) as conn:
            try:
                conn.execute("BEGIN IMMEDIATE")
                self._ensure_operations(conn)
                previous = conn.execute(
                    "SELECT quantity, atk_gain FROM permanent_atk_item_operations "
                    "WHERE operation_id=%s",
                    (operation_id,),
                ).fetchone()
                if previous is not None:
                    conn.rollback()
                    return result("duplicate", previous)
                if conn.execute(
                    "SELECT 1 FROM BuffInfo WHERE user_id=%s", (user_id,)
                ).fetchone() is None:
                    conn.rollback()
                    return result("buff_missing")
                item = conn.execute(
                    "SELECT goods_num FROM back WHERE user_id=%s AND goods_id=%s",
                    (user_id, item_id),
                ).fetchone()
                if item is None or int(item[0] or 0) < quantity:
                    conn.rollback()
                    return result("item_insufficient")

                columns = set(conn.column_names("back"))
                updates = ["goods_num=goods_num-%s"]
                params: list[object] = [quantity]
                if "day_num" in columns:
                    updates.append("day_num=COALESCE(day_num, 0)+%s")
                    params.append(quantity)
                if "all_num" in columns:
                    updates.append("all_num=COALESCE(all_num, 0)+%s")
                    params.append(quantity)
                if "bind_num" in columns:
                    updates.append(
                        "bind_num=CASE WHEN goods_num-%s=0 THEN 0 "
                        "WHEN COALESCE(bind_num, 0)>=%s "
                        "THEN COALESCE(bind_num, 0)-%s "
                        "ELSE MIN(COALESCE(bind_num, 0), goods_num-%s) END"
                    )
                    params.extend((quantity, quantity, quantity, quantity))
                consumed = conn.execute(
                    f"UPDATE back SET {', '.join(updates)} "
                    "WHERE user_id=%s AND goods_id=%s AND goods_num>=%s",
                    (*params, user_id, item_id, quantity),
                )
                updated = conn.execute(
                    "UPDATE BuffInfo SET atk_buff=COALESCE(atk_buff, 0)+%s "
                    "WHERE user_id=%s",
                    (atk_gain, user_id),
                )
                if consumed.rowcount != 1 or updated.rowcount != 1:
                    conn.rollback()
                    return result("state_changed")
                conn.execute(
                    "INSERT INTO permanent_atk_item_operations "
                    "(operation_id, user_id, item_id, quantity, atk_gain) "
                    "VALUES (%s, %s, %s, %s, %s)",
                    (operation_id, user_id, item_id, quantity, atk_gain),
                )
                conn.commit()
                return result("applied")
            except Exception:
                conn.rollback()
                raise

@dataclass(frozen=True)
class RecoveryItemUse:
    status: str
    user_id: str
    item_id: int
    quantity: int
    hp_before: int
    hp_after: int
    mp_before: int
    mp_after: int
    stamina_before: int
    stamina_after: int

    @property
    def succeeded(self) -> bool:
        return self.status in {"applied", "duplicate"}

class RecoveryItemService:
    """Consume recovery elixirs and update character state atomically."""

    def __init__(self, database: str | Path, lock: RLock | None = None) -> None:
        self._database = Path(database)
        self._lock = lock or RLock()

    @staticmethod
    def _ensure_operations(conn) -> None:
        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS recovery_item_operations (
                operation_id TEXT PRIMARY KEY,
                user_id TEXT NOT NULL,
                item_id INTEGER NOT NULL,
                quantity INTEGER NOT NULL,
                mode TEXT NOT NULL,
                hp_before INTEGER NOT NULL,
                hp_after INTEGER NOT NULL,
                mp_before INTEGER NOT NULL,
                mp_after INTEGER NOT NULL,
                stamina_before INTEGER NOT NULL,
                stamina_after INTEGER NOT NULL,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
            """
        )

    def apply(
        self,
        operation_id,
        user_id,
        item_id,
        quantity,
        *,
        mode,
        hp_gain=0,
        mp_gain=0,
        atk_after=None,
        stamina_gain=0,
        max_stamina=0,
    ) -> RecoveryItemUse:
        operation_id = str(operation_id).strip()
        if not operation_id:
            raise ValueError("operation_id must not be empty")
        user_id = str(user_id)
        item_id = int(item_id)
        quantity = int(quantity)
        mode = str(mode)
        if quantity <= 0:
            raise ValueError("quantity must be positive")
        if mode not in {"hp_mp", "full", "stamina"}:
            raise ValueError("unsupported recovery mode")

        def result(status: str, values=None) -> RecoveryItemUse:
            values = values or (quantity, 0, 0, 0, 0, 0, 0)
            return RecoveryItemUse(status, user_id, item_id, *(int(v) for v in values))

        with self._lock, closing(db_backend.connect(self._database)) as conn:
            try:
                conn.execute("BEGIN IMMEDIATE")
                self._ensure_operations(conn)
                previous = conn.execute(
                    "SELECT quantity, hp_before, hp_after, mp_before, mp_after, "
                    "stamina_before, stamina_after FROM recovery_item_operations "
                    "WHERE operation_id=%s",
                    (operation_id,),
                ).fetchone()
                if previous is not None:
                    conn.rollback()
                    return result("duplicate", previous)

                user = conn.execute(
                    "SELECT exp, hp, mp, atk, user_stamina FROM user_xiuxian "
                    "WHERE user_id=%s",
                    (user_id,),
                ).fetchone()
                if user is None:
                    conn.rollback()
                    return result("user_missing")
                item = conn.execute(
                    "SELECT goods_num FROM back WHERE user_id=%s AND goods_id=%s",
                    (user_id, item_id),
                ).fetchone()
                if item is None or int(item[0] or 0) < quantity:
                    conn.rollback()
                    return result("item_insufficient")

                exp, hp_before, mp_before, atk_before, stamina_before = (
                    int(value or 0) for value in user
                )
                next_hp = hp_before
                next_mp = mp_before
                next_atk = atk_before
                next_stamina = stamina_before
                if mode == "hp_mp":
                    max_hp = int(exp / 2)
                    next_hp = (
                        min(hp_before + int(hp_gain), max_hp)
                        if hp_before < max_hp
                        else hp_before
                    )
                    next_mp = (
                        min(mp_before + int(mp_gain), exp)
                        if mp_before < exp
                        else mp_before
                    )
                elif mode == "full":
                    next_hp = int(exp / 2)
                    next_mp = exp
                    next_atk = int(exp / 10) if atk_after is None else int(atk_after)
                elif mode == "stamina":
                    next_stamina = min(
                        stamina_before + int(stamina_gain), int(max_stamina)
                    )

                columns = set(conn.column_names("back"))
                updates = ["goods_num=goods_num-%s"]
                params: list[object] = [quantity]
                if "day_num" in columns:
                    updates.append("day_num=COALESCE(day_num, 0)+%s")
                    params.append(quantity)
                if "all_num" in columns:
                    updates.append("all_num=COALESCE(all_num, 0)+%s")
                    params.append(quantity)
                if "bind_num" in columns:
                    updates.append(
                        "bind_num=CASE WHEN goods_num-%s=0 THEN 0 "
                        "WHEN COALESCE(bind_num, 0)>=%s "
                        "THEN COALESCE(bind_num, 0)-%s "
                        "ELSE MIN(COALESCE(bind_num, 0), goods_num-%s) END"
                    )
                    params.extend((quantity, quantity, quantity, quantity))
                consumed = conn.execute(
                    f"UPDATE back SET {', '.join(updates)} "
                    "WHERE user_id=%s AND goods_id=%s AND goods_num>=%s",
                    (*params, user_id, item_id, quantity),
                )
                if mode == "stamina":
                    updated = conn.execute(
                        "UPDATE user_xiuxian SET user_stamina=%s WHERE user_id=%s",
                        (next_stamina, user_id),
                    )
                else:
                    updated = conn.execute(
                        "UPDATE user_xiuxian SET hp=%s, mp=%s, atk=%s WHERE user_id=%s",
                        (next_hp, next_mp, next_atk, user_id),
                    )
                if consumed.rowcount != 1 or updated.rowcount != 1:
                    conn.rollback()
                    return result("state_changed")

                values = (
                    quantity,
                    hp_before,
                    next_hp,
                    mp_before,
                    next_mp,
                    stamina_before,
                    next_stamina,
                )
                conn.execute(
                    "INSERT INTO recovery_item_operations "
                    "(operation_id, user_id, item_id, quantity, mode, hp_before, "
                    "hp_after, mp_before, mp_after, stamina_before, stamina_after) "
                    "VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)",
                    (operation_id, user_id, item_id, quantity, mode, *values[1:]),
                )
                conn.commit()
                return result("applied", values)
            except Exception:
                conn.rollback()
                raise

SKILL_COLUMNS = {
    "功法": "main_buff",
    "辅修功法": "sub_buff",
    "神通": "sec_buff",
    "身法": "effect1_buff",
    "瞳术": "effect2_buff",
}

@dataclass(frozen=True)
class SkillLearningResult:
    status: str
    user_id: str
    skill_item_id: int
    skill_type: str
    previous_item_id: int = 0

    @property
    def succeeded(self) -> bool:
        return self.status in {"learned", "duplicate"}

class SkillLearningService:
    """Consume a skill book and update the matching skill slot atomically."""

    def __init__(self, database: str | Path, lock: RLock | None = None) -> None:
        self._database = Path(database)
        self._lock = lock or RLock()

    @staticmethod
    def _ensure_operations(conn) -> None:
        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS skill_learning_operations (
                operation_id TEXT PRIMARY KEY,
                user_id TEXT NOT NULL,
                skill_item_id INTEGER NOT NULL,
                skill_type TEXT NOT NULL,
                previous_item_id INTEGER NOT NULL,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
            """
        )

    def learn(self, operation_id, user_id, skill_item_id, skill_type) -> SkillLearningResult:
        operation_id = str(operation_id).strip()
        if not operation_id:
            raise ValueError("operation_id must not be empty")
        user_id = str(user_id)
        skill_item_id = int(skill_item_id)
        skill_type = str(skill_type)
        try:
            column = SKILL_COLUMNS[skill_type]
        except KeyError as exc:
            raise ValueError(f"unsupported skill type: {skill_type}") from exc

        def result(status: str, previous_item_id=0) -> SkillLearningResult:
            return SkillLearningResult(
                status, user_id, skill_item_id, skill_type, int(previous_item_id or 0)
            )

        with self._lock, closing(db_backend.connect(self._database)) as conn:
            try:
                conn.execute("BEGIN IMMEDIATE")
                self._ensure_operations(conn)
                previous = conn.execute(
                    "SELECT previous_item_id FROM skill_learning_operations "
                    "WHERE operation_id=%s",
                    (operation_id,),
                ).fetchone()
                if previous is not None:
                    conn.rollback()
                    return result("duplicate", previous[0])

                inventory = conn.execute(
                    "SELECT goods_num FROM back WHERE user_id=%s AND goods_id=%s",
                    (user_id, skill_item_id),
                ).fetchone()
                if inventory is None or int(inventory[0] or 0) <= 0:
                    conn.rollback()
                    return result("item_missing")
                buff = conn.execute(
                    f"SELECT {column} FROM BuffInfo WHERE user_id=%s", (user_id,)
                ).fetchone()
                if buff is None:
                    conn.rollback()
                    return result("buff_missing")
                previous_item_id = int(buff[0] or 0)
                if previous_item_id == skill_item_id:
                    conn.rollback()
                    return result("already_learned", previous_item_id)

                columns = set(conn.column_names("back"))
                updates = ["goods_num=goods_num-1"]
                if "bind_num" in columns:
                    updates.append(
                        "bind_num=CASE WHEN goods_num-1=0 THEN 0 "
                        "WHEN COALESCE(bind_num, 0)>0 THEN COALESCE(bind_num, 0)-1 "
                        "ELSE MIN(COALESCE(bind_num, 0), goods_num-1) END"
                    )
                consumed = conn.execute(
                    f"UPDATE back SET {', '.join(updates)} "
                    "WHERE user_id=%s AND goods_id=%s AND goods_num>0",
                    (user_id, skill_item_id),
                )
                updated = conn.execute(
                    f"UPDATE BuffInfo SET {column}=%s WHERE user_id=%s",
                    (skill_item_id, user_id),
                )
                if consumed.rowcount != 1 or updated.rowcount != 1:
                    conn.rollback()
                    return result("state_changed", previous_item_id)
                conn.execute(
                    "INSERT INTO skill_learning_operations "
                    "(operation_id, user_id, skill_item_id, skill_type, previous_item_id) "
                    "VALUES (%s, %s, %s, %s, %s)",
                    (operation_id, user_id, skill_item_id, skill_type, previous_item_id),
                )
                conn.commit()
                return result("learned", previous_item_id)
            except Exception:
                conn.rollback()
                raise

@dataclass(frozen=True)
class StoneItemReward:
    status: str
    user_id: str
    item_id: int
    quantity: int
    rewards: tuple[int, ...] = ()

    @property
    def total_stone(self) -> int:
        return sum(self.rewards)

    @property
    def succeeded(self) -> bool:
        return self.status in {"applied", "duplicate"}

class StoneItemRewardService:
    """Consume an item and grant a pre-rolled stone reward atomically."""

    def __init__(self, database: str | Path, lock: RLock | None = None) -> None:
        self._database = Path(database)
        self._lock = lock or RLock()

    @staticmethod
    def _ensure_operations(conn) -> None:
        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS stone_item_reward_operations (
                operation_id TEXT PRIMARY KEY,
                user_id TEXT NOT NULL,
                reward_type TEXT NOT NULL,
                item_id INTEGER NOT NULL,
                quantity INTEGER NOT NULL,
                rewards_json TEXT NOT NULL,
                total_stone INTEGER NOT NULL,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
            """
        )

    def apply(
        self,
        operation_id,
        user_id,
        *,
        reward_type: str,
        item_id: int,
        rewards: list[int] | tuple[int, ...],
    ) -> StoneItemReward:
        operation_id = str(operation_id).strip()
        if not operation_id:
            raise ValueError("operation_id must not be empty")
        user_id = str(user_id)
        item_id = int(item_id)
        fixed_rewards = tuple(int(value) for value in rewards)
        if not fixed_rewards or any(value < 0 for value in fixed_rewards):
            raise ValueError("rewards must contain non-negative values")
        quantity = len(fixed_rewards)

        with self._lock, closing(db_backend.connect(self._database)) as conn:
            try:
                conn.execute("BEGIN IMMEDIATE")
                self._ensure_operations(conn)
                previous = conn.execute(
                    """
                    SELECT user_id, item_id, quantity, rewards_json
                    FROM stone_item_reward_operations WHERE operation_id=%s
                    """,
                    (operation_id,),
                ).fetchone()
                if previous is not None:
                    conn.rollback()
                    return StoneItemReward(
                        "duplicate",
                        str(previous[0]),
                        int(previous[1]),
                        int(previous[2]),
                        tuple(int(value) for value in json.loads(previous[3])),
                    )

                user = conn.execute(
                    "SELECT stone FROM user_xiuxian WHERE user_id=%s", (user_id,)
                ).fetchone()
                if user is None:
                    conn.rollback()
                    return StoneItemReward("user_missing", user_id, item_id, quantity)
                item = conn.execute(
                    "SELECT goods_num FROM back WHERE user_id=%s AND goods_id=%s",
                    (user_id, item_id),
                ).fetchone()
                if item is None or int(item[0] or 0) < quantity:
                    conn.rollback()
                    return StoneItemReward("item_insufficient", user_id, item_id, quantity)

                columns = set(conn.column_names("back"))
                bind_update = ""
                if "bind_num" in columns:
                    bind_update = ", bind_num=MIN(COALESCE(bind_num, 0), goods_num-%s)"
                consumed = conn.execute(
                    f"""
                    UPDATE back SET goods_num=goods_num-%s{bind_update}
                    WHERE user_id=%s AND goods_id=%s AND goods_num >= %s
                    """,
                    ((quantity, quantity, user_id, item_id, quantity) if bind_update else
                     (quantity, user_id, item_id, quantity)),
                )
                granted = conn.execute(
                    "UPDATE user_xiuxian SET stone=stone+%s WHERE user_id=%s",
                    (sum(fixed_rewards), user_id),
                )
                if consumed.rowcount != 1:
                    conn.rollback()
                    return StoneItemReward("item_changed", user_id, item_id, quantity)
                if granted.rowcount != 1:
                    conn.rollback()
                    return StoneItemReward("user_changed", user_id, item_id, quantity)

                conn.execute(
                    """
                    INSERT INTO stone_item_reward_operations (
                        operation_id, user_id, reward_type, item_id, quantity,
                        rewards_json, total_stone
                    ) VALUES (%s, %s, %s, %s, %s, %s, %s)
                    """,
                    (
                        operation_id,
                        user_id,
                        str(reward_type),
                        item_id,
                        quantity,
                        json.dumps(fixed_rewards),
                        sum(fixed_rewards),
                    ),
                )
                conn.commit()
                return StoneItemReward(
                    "applied", user_id, item_id, quantity, fixed_rewards
                )
            except Exception:
                conn.rollback()
                raise

@dataclass(frozen=True)
class ThreeCultivationPillUse:
    status: str
    user_id: str
    item_id: int
    quantity: int
    requested_exp: int
    exp_gain: int
    hp_before: int
    hp_after: int
    mp_before: int
    mp_after: int

    @property
    def succeeded(self) -> bool:
        return self.status in {"applied", "duplicate"}

class ThreeCultivationPillService:
    """Apply capped pill cultivation, recovery, and consumption atomically."""

    def __init__(self, database: str | Path, lock: RLock | None = None) -> None:
        self._database = Path(database)
        self._lock = lock or RLock()

    @staticmethod
    def _ensure_operations(conn) -> None:
        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS three_cultivation_pill_operations (
                operation_id TEXT PRIMARY KEY,
                user_id TEXT NOT NULL,
                item_id INTEGER NOT NULL,
                quantity INTEGER NOT NULL,
                requested_exp INTEGER NOT NULL,
                exp_gain INTEGER NOT NULL,
                hp_before INTEGER NOT NULL,
                hp_after INTEGER NOT NULL,
                mp_before INTEGER NOT NULL,
                mp_after INTEGER NOT NULL,
                power_multiplier REAL NOT NULL,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
            """
        )

    def apply(
        self,
        operation_id,
        user_id,
        item_id,
        quantity,
        requested_exp,
        *,
        max_exp,
        power_multiplier,
    ) -> ThreeCultivationPillUse:
        operation_id = str(operation_id).strip()
        if not operation_id:
            raise ValueError("operation_id must not be empty")
        user_id = str(user_id)
        item_id = int(item_id)
        quantity = int(quantity)
        requested_exp = int(requested_exp)
        max_exp = int(max_exp)
        power_multiplier = float(power_multiplier)
        if quantity <= 0 or requested_exp < 0 or max_exp < 0:
            raise ValueError("quantity must be positive and experience non-negative")
        if power_multiplier < 0:
            raise ValueError("power_multiplier must be non-negative")

        def result(status: str, values=None) -> ThreeCultivationPillUse:
            if values is None:
                values = (quantity, requested_exp, 0, 0, 0, 0, 0)
            return ThreeCultivationPillUse(
                status,
                user_id,
                item_id,
                *(int(value) for value in values),
            )

        with self._lock, closing(db_backend.connect(self._database)) as conn:
            try:
                conn.execute("BEGIN IMMEDIATE")
                self._ensure_operations(conn)
                previous = conn.execute(
                    "SELECT quantity, requested_exp, exp_gain, hp_before, hp_after, "
                    "mp_before, mp_after FROM three_cultivation_pill_operations "
                    "WHERE operation_id=%s",
                    (operation_id,),
                ).fetchone()
                if previous is not None:
                    conn.rollback()
                    return result("duplicate", previous)

                user = conn.execute(
                    "SELECT exp, hp, mp FROM user_xiuxian WHERE user_id=%s",
                    (user_id,),
                ).fetchone()
                if user is None:
                    conn.rollback()
                    return result("user_missing")
                item = conn.execute(
                    "SELECT goods_num FROM back WHERE user_id=%s AND goods_id=%s",
                    (user_id, item_id),
                ).fetchone()
                if item is None or int(item[0] or 0) < quantity:
                    conn.rollback()
                    return result("item_insufficient")

                current_exp, hp_before, mp_before = (int(value or 0) for value in user)
                exp_gain = min(requested_exp, max(0, max_exp - current_exp))
                final_exp = current_exp + exp_gain
                max_hp = int(final_exp / 2)
                hp_after = (
                    min(hp_before + int(current_exp / 10), max_hp)
                    if hp_before < max_hp
                    else hp_before
                )
                mp_after = (
                    min(mp_before + int(current_exp / 20), final_exp)
                    if mp_before < final_exp
                    else mp_before
                )

                back_columns = set(conn.column_names("back"))
                bind_update = ""
                params: list[object] = [quantity]
                if "bind_num" in back_columns:
                    bind_update = (
                        ", bind_num=CASE WHEN goods_num-%s=0 THEN 0 "
                        "WHEN COALESCE(bind_num, 0)>=%s "
                        "THEN COALESCE(bind_num, 0)-%s "
                        "ELSE MIN(COALESCE(bind_num, 0), goods_num-%s) END"
                    )
                    params.extend((quantity, quantity, quantity, quantity))
                consumed = conn.execute(
                    f"UPDATE back SET goods_num=goods_num-%s{bind_update} "
                    "WHERE user_id=%s AND goods_id=%s AND goods_num>=%s",
                    (*params, user_id, item_id, quantity),
                )
                updated = conn.execute(
                    "UPDATE user_xiuxian SET exp=%s, hp=%s, mp=%s, "
                    "power=ROUND(%s*%s, 0) WHERE user_id=%s",
                    (
                        final_exp,
                        hp_after,
                        mp_after,
                        final_exp,
                        power_multiplier,
                        user_id,
                    ),
                )
                if consumed.rowcount != 1 or updated.rowcount != 1:
                    conn.rollback()
                    return result("state_changed")

                values = (
                    quantity,
                    requested_exp,
                    exp_gain,
                    hp_before,
                    hp_after,
                    mp_before,
                    mp_after,
                )
                conn.execute(
                    "INSERT INTO three_cultivation_pill_operations "
                    "(operation_id, user_id, item_id, quantity, requested_exp, "
                    "exp_gain, hp_before, hp_after, mp_before, mp_after, "
                    "power_multiplier) VALUES (%s, %s, %s, %s, %s, %s, %s, "
                    "%s, %s, %s, %s)",
                    (operation_id, user_id, item_id, *values, power_multiplier),
                )
                conn.commit()
                return result("applied", values)
            except Exception:
                conn.rollback()
                raise

@dataclass(frozen=True)
class UnbindItemResult:
    status: str
    user_id: str
    charm_item_id: int
    target_item_id: int
    quantity: int

    @property
    def succeeded(self) -> bool:
        return self.status in {"applied", "duplicate"}

class UnbindItemService:
    """Consume charms and reduce a target item's bound quantity atomically."""

    def __init__(self, database: str | Path, lock: RLock | None = None) -> None:
        self._database = Path(database)
        self._lock = lock or RLock()

    @staticmethod
    def _ensure_operations(conn) -> None:
        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS unbind_item_operations (
                operation_id TEXT PRIMARY KEY,
                user_id TEXT NOT NULL,
                charm_item_id INTEGER NOT NULL,
                target_item_id INTEGER NOT NULL,
                quantity INTEGER NOT NULL,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
            """
        )

    def apply(
        self,
        operation_id,
        user_id,
        charm_item_id,
        target_item_id,
        requested_quantity,
    ) -> UnbindItemResult:
        operation_id = str(operation_id).strip()
        if not operation_id:
            raise ValueError("operation_id must not be empty")
        user_id = str(user_id)
        charm_item_id = int(charm_item_id)
        target_item_id = int(target_item_id)
        requested_quantity = int(requested_quantity)
        if requested_quantity <= 0:
            raise ValueError("requested_quantity must be positive")
        if charm_item_id == target_item_id:
            raise ValueError("charm and target item must differ")

        def result(status: str, quantity=0) -> UnbindItemResult:
            return UnbindItemResult(
                status, user_id, charm_item_id, target_item_id, int(quantity)
            )

        with self._lock, closing(db_backend.connect(self._database)) as conn:
            try:
                conn.execute("BEGIN IMMEDIATE")
                self._ensure_operations(conn)
                previous = conn.execute(
                    "SELECT quantity FROM unbind_item_operations WHERE operation_id=%s",
                    (operation_id,),
                ).fetchone()
                if previous is not None:
                    conn.rollback()
                    return result("duplicate", previous[0])

                charm = conn.execute(
                    "SELECT goods_num FROM back WHERE user_id=%s AND goods_id=%s",
                    (user_id, charm_item_id),
                ).fetchone()
                if charm is None or int(charm[0] or 0) <= 0:
                    conn.rollback()
                    return result("charm_missing")
                target = conn.execute(
                    "SELECT goods_num, bind_num FROM back WHERE user_id=%s AND goods_id=%s",
                    (user_id, target_item_id),
                ).fetchone()
                if target is None or int(target[0] or 0) <= 0:
                    conn.rollback()
                    return result("target_missing")
                if int(target[1] or 0) <= 0:
                    conn.rollback()
                    return result("not_bound")

                quantity = min(
                    requested_quantity,
                    int(charm[0] or 0),
                    int(target[1] or 0),
                )
                charm_columns = set(conn.column_names("back"))
                updates = ["goods_num=goods_num-%s"]
                params: list[object] = [quantity]
                if "bind_num" in charm_columns:
                    updates.append(
                        "bind_num=CASE WHEN goods_num-%s=0 THEN 0 "
                        "WHEN COALESCE(bind_num, 0)>=%s "
                        "THEN COALESCE(bind_num, 0)-%s "
                        "ELSE MIN(COALESCE(bind_num, 0), goods_num-%s) END"
                    )
                    params.extend((quantity, quantity, quantity, quantity))
                consumed = conn.execute(
                    f"UPDATE back SET {', '.join(updates)} "
                    "WHERE user_id=%s AND goods_id=%s AND goods_num>=%s",
                    (*params, user_id, charm_item_id, quantity),
                )
                unbound = conn.execute(
                    "UPDATE back SET bind_num=bind_num-%s "
                    "WHERE user_id=%s AND goods_id=%s AND bind_num>=%s",
                    (quantity, user_id, target_item_id, quantity),
                )
                if consumed.rowcount != 1 or unbound.rowcount != 1:
                    conn.rollback()
                    return result("state_changed")
                conn.execute(
                    "INSERT INTO unbind_item_operations "
                    "(operation_id, user_id, charm_item_id, target_item_id, quantity) "
                    "VALUES (%s, %s, %s, %s, %s)",
                    (operation_id, user_id, charm_item_id, target_item_id, quantity),
                )
                conn.commit()
                return result("applied", quantity)
            except Exception:
                conn.rollback()
                raise

__all__ = [
    "PackageReward",
    "PackageOpenResult",
    "PackageRewardService",
    "AccessoryPackageResult",
    "AccessoryPackageService",
    "AccessoryTransactionResult",
    "AccessoryTransactionService",
    "AlchemyResult",
    "AlchemyService",
    "BackpackRepairResult",
    "BackpackRepairService",
    "BatchPetEggUseResult",
    "BatchItemUseService",
    "BlessedFlagReplaceResult",
    "BlessedFlagReplaceService",
    "BreakthroughRateItemUse",
    "BreakthroughRateItemService",
    "CultivationItemUse",
    "CultivationItemService",
    "EquipmentChange",
    "EquipmentService",
    "LotteryReward",
    "LotteryTalismanUse",
    "LotteryTalismanService",
    "PermanentAtkItemUse",
    "PermanentAtkItemService",
    "RecoveryItemUse",
    "RecoveryItemService",
    "SkillLearningResult",
    "SkillLearningService",
    "StoneItemReward",
    "StoneItemRewardService",
    "ThreeCultivationPillUse",
    "ThreeCultivationPillService",
    "UnbindItemResult",
    "UnbindItemService",
    "SKILL_COLUMNS",
]
