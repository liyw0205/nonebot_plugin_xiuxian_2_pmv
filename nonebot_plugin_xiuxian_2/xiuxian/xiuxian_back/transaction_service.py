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
from ...compatibility.legacy_back_accessory_transaction import (
    AccessoryTransactionResult,
    AccessoryTransactionService,
)
from ...compatibility.legacy_back_skill_learning import (
    SKILL_COLUMNS,
    SkillLearningResult,
    SkillLearningService,
)
from ...compatibility.legacy_back_lottery_talisman import (
    LotteryReward,
    LotteryTalismanUse,
    LotteryTalismanService,
)

from ...features.back.pet_egg_repository import BatchPetEggUseResult, PetEggUseSqlRepository
from ...compatibility.legacy_back_repair import BackpackRepairResult, BackpackRepairService
from ...compatibility.legacy_back_alchemy import AlchemyResult, AlchemyService
from ...compatibility.legacy_back_package_reward import (
    PackageOpenResult,
    PackageReward,
    PackageRewardService,
)
from ...compatibility.legacy_back_accessory_package import (
    AccessoryPackageResult,
    AccessoryPackageService,
)
from ...compatibility.legacy_back_pet_egg import BatchItemUseService
from ...compatibility.legacy_back_equipment import EquipmentChange, EquipmentService


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
                    SET exp=CAST(COALESCE(exp,0) AS REAL)+CAST(%s AS REAL),
                        hp=CAST(COALESCE(hp,0) AS REAL)+CAST(%s AS REAL),
                        mp=CAST(COALESCE(mp,0) AS REAL)+CAST(%s AS REAL),
                        atk=CAST(COALESCE(atk,0) AS REAL)+CAST(%s AS REAL),
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
                    "UPDATE user_xiuxian SET stone=CAST(COALESCE(stone,0) AS REAL)+CAST(%s AS REAL) WHERE user_id=%s",
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
