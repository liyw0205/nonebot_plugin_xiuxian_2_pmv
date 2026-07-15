from __future__ import annotations

import json
from contextlib import closing
from dataclasses import dataclass, field
from pathlib import Path
from threading import RLock
from ..xiuxian_utils import db_backend
import random

@dataclass(frozen=True)
class NatalTrainingResult:
    status: str
    exp_added: int
    stone_cost: int
    level: int
    exp: int
    max_exp: int

    @property
    def succeeded(self) -> bool:
        return self.status in {"trained", "duplicate"}

class NatalTrainingService:
    """Charge stones and train a natal treasure across attached databases."""

    def __init__(self, game_database: str | Path, player_database: str | Path,
                 lock: RLock | None = None) -> None:
        self._game_database = Path(game_database)
        self._player_database = Path(player_database)
        self._lock = lock or RLock()

    def get_result(self, operation_id: str) -> NatalTrainingResult | None:
        operation_id = str(operation_id).strip()
        if not operation_id:
            return None
        with self._lock, closing(db_backend.connect(self._game_database)) as conn:
            conn.execute(
                "CREATE TABLE IF NOT EXISTS natal_training_operations ("
                "operation_id TEXT PRIMARY KEY, user_id TEXT NOT NULL, requested_exp INTEGER NOT NULL, "
                "base_cost INTEGER NOT NULL, growth_rate REAL NOT NULL, max_level INTEGER NOT NULL, "
                "max_exp_base INTEGER NOT NULL, max_exp_growth INTEGER NOT NULL, exp_added INTEGER NOT NULL, "
                "stone_cost INTEGER NOT NULL, level INTEGER NOT NULL, exp INTEGER NOT NULL, max_exp INTEGER NOT NULL)"
            )
            previous = conn.execute(
                "SELECT exp_added, stone_cost, level, exp, max_exp FROM natal_training_operations WHERE operation_id=%s",
                (operation_id,),
            ).fetchone()
            if previous is None:
                return None
            return NatalTrainingResult("duplicate", int(previous[0]), int(previous[1]), int(previous[2]), int(previous[3]), int(previous[4]))

    def train(self, operation_id, user_id, requested_exp, *, base_cost, growth_rate,
              max_level, max_exp_base, max_exp_growth) -> NatalTrainingResult:
        operation_id = str(operation_id).strip()
        user_id = str(user_id)
        requested_exp = int(requested_exp)
        base_cost = int(base_cost)
        growth_rate = float(growth_rate)
        max_level = int(max_level)
        max_exp_base = int(max_exp_base)
        max_exp_growth = int(max_exp_growth)
        if not operation_id or requested_exp <= 0 or base_cost <= 0 or max_level <= 0:
            raise ValueError("valid operation and positive training parameters are required")

        def result(status, exp_added=0, stone_cost=0, level=0, exp=0, max_exp=0):
            return NatalTrainingResult(
                status, int(exp_added), int(stone_cost), int(level), int(exp), int(max_exp)
            )

        with self._lock, closing(db_backend.connect(self._game_database)) as conn:
            conn.execute("ATTACH DATABASE %s AS player_data", (str(self._player_database),))
            try:
                conn.execute("BEGIN IMMEDIATE")
                conn.execute(
                    "CREATE TABLE IF NOT EXISTS natal_training_operations ("
                    "operation_id TEXT PRIMARY KEY, user_id TEXT NOT NULL, requested_exp INTEGER NOT NULL, "
                    "base_cost INTEGER NOT NULL, growth_rate REAL NOT NULL, max_level INTEGER NOT NULL, "
                    "max_exp_base INTEGER NOT NULL, max_exp_growth INTEGER NOT NULL, exp_added INTEGER NOT NULL, "
                    "stone_cost INTEGER NOT NULL, level INTEGER NOT NULL, exp INTEGER NOT NULL, max_exp INTEGER NOT NULL)"
                )
                conn.execute("CREATE TABLE IF NOT EXISTS player_data.natal_treasure (user_id TEXT PRIMARY KEY)")
                columns = {
                    str(row[1]) for row in conn.execute(
                        "PRAGMA player_data.table_info(natal_treasure)"
                    ).fetchall()
                }
                for field in ("form", "level", "exp", "max_exp"):
                    if field not in columns:
                        conn.execute(
                            f"ALTER TABLE player_data.natal_treasure ADD COLUMN {db_backend.quote_ident(field)} INTEGER"
                        )
                previous = conn.execute(
                    "SELECT user_id, requested_exp, base_cost, growth_rate, max_level, max_exp_base, "
                    "max_exp_growth, exp_added, stone_cost, level, exp, max_exp "
                    "FROM natal_training_operations WHERE operation_id=%s", (operation_id,),
                ).fetchone()
                if previous is not None:
                    conn.rollback()
                    # Request identity = user + requested_exp; costs/outcomes stored in op row.
                    if str(previous[0]) != user_id or int(previous[1]) != requested_exp:
                        return result("state_changed")
                    return result("duplicate", *previous[7:])

                user = conn.execute(
                    "SELECT COALESCE(stone, 0) FROM user_xiuxian WHERE user_id=%s", (user_id,)
                ).fetchone()
                treasure = conn.execute(
                    "SELECT COALESCE(form, 0), COALESCE(level, 0), COALESCE(exp, 0), "
                    "COALESCE(max_exp, %s) FROM player_data.natal_treasure WHERE user_id=%s",
                    (max_exp_base, user_id),
                ).fetchone()
                if user is None:
                    conn.rollback()
                    return result("user_missing")
                if treasure is None or int(treasure[0]) == 0:
                    conn.rollback()
                    return result("treasure_missing")
                level, current_exp = int(treasure[1]), int(treasure[2])
                current_max_exp = max_exp_base + level * max_exp_growth
                if level >= max_level:
                    conn.rollback()
                    return result("max_level", level=level, exp=current_exp, max_exp=current_max_exp)
                exp_added = min(requested_exp, max(0, current_max_exp - current_exp))
                if exp_added <= 0:
                    conn.rollback()
                    return result("exp_full", level=level, exp=current_exp, max_exp=current_max_exp)
                stone_cost = int(base_cost * (1 + level * growth_rate)) * exp_added
                if int(user[0]) < stone_cost:
                    conn.rollback()
                    return result("stone_insufficient", exp_added=exp_added, stone_cost=stone_cost,
                                  level=level, exp=current_exp, max_exp=current_max_exp)

                new_exp = current_exp + exp_added
                new_level = level
                new_max_exp = current_max_exp
                if new_exp >= current_max_exp:
                    new_level += 1
                    new_exp -= current_max_exp
                    if new_level < max_level:
                        new_max_exp = max_exp_base + new_level * max_exp_growth
                    else:
                        new_exp = current_max_exp
                charged = conn.execute(
                    "UPDATE user_xiuxian SET stone=stone-%s WHERE user_id=%s AND stone>=%s",
                    (stone_cost, user_id, stone_cost),
                )
                updated = conn.execute(
                    "UPDATE player_data.natal_treasure SET level=%s, exp=%s, max_exp=%s WHERE user_id=%s",
                    (new_level, new_exp, new_max_exp, user_id),
                )
                if charged.rowcount != 1 or updated.rowcount != 1:
                    conn.rollback()
                    return result("state_changed")
                conn.execute(
                    "INSERT INTO natal_training_operations VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)",
                    (operation_id, user_id, requested_exp, base_cost, growth_rate, max_level,
                     max_exp_base, max_exp_growth, exp_added, stone_cost, new_level, new_exp, new_max_exp),
                )
                conn.commit()
                return result("trained", exp_added, stone_cost, new_level, new_exp, new_max_exp)
            except Exception:
                conn.rollback()
                raise
            finally:
                conn.execute("DETACH DATABASE player_data")

@dataclass(frozen=True)
class EffectUpgradeResult:
    status: str
    slot: int
    effect_type: int
    level: int

    @property
    def succeeded(self) -> bool:
        return self.status in {"upgraded", "duplicate"}

class EffectUpgradeService:
    """Consume a scripture and upgrade one lowest-level effect atomically."""

    def __init__(self, game_database: str | Path, player_database: str | Path,
                 lock: RLock | None = None) -> None:
        self._game_database = Path(game_database)
        self._player_database = Path(player_database)
        self._lock = lock or RLock()

    def get_result(self, operation_id: str) -> EffectUpgradeResult | None:
        operation_id = str(operation_id).strip()
        if not operation_id:
            return None
        with self._lock, closing(db_backend.connect(self._game_database)) as conn:
            conn.execute(
                "CREATE TABLE IF NOT EXISTS natal_effect_upgrade_operations ("
                "operation_id TEXT PRIMARY KEY, user_id TEXT NOT NULL, scripture_id INTEGER NOT NULL, "
                "scripture_cost INTEGER NOT NULL, max_slots INTEGER NOT NULL, max_effect_level INTEGER NOT NULL, "
                "choice_seed INTEGER NOT NULL, slot INTEGER NOT NULL, effect_type INTEGER NOT NULL, "
                "level INTEGER NOT NULL)"
            )
            previous = conn.execute(
                "SELECT slot, effect_type, level FROM natal_effect_upgrade_operations WHERE operation_id=%s",
                (operation_id,),
            ).fetchone()
            if previous is None:
                return None
            return EffectUpgradeResult("duplicate", int(previous[0]), int(previous[1]), int(previous[2]))

    def upgrade(self, operation_id, user_id, scripture_id, scripture_cost,
                max_slots, max_effect_level, choice_seed) -> EffectUpgradeResult:
        operation_id = str(operation_id).strip()
        user_id = str(user_id)
        scripture_id = int(scripture_id)
        scripture_cost = int(scripture_cost)
        max_slots = int(max_slots)
        max_effect_level = int(max_effect_level)
        choice_seed = int(choice_seed)
        if not operation_id or scripture_cost <= 0 or max_slots <= 0 or max_effect_level <= 0:
            raise ValueError("valid operation and positive upgrade parameters are required")

        def result(status, slot=0, effect_type=0, level=0):
            return EffectUpgradeResult(status, int(slot), int(effect_type), int(level))

        with self._lock, closing(db_backend.connect(self._game_database)) as conn:
            conn.execute("ATTACH DATABASE %s AS player_data", (str(self._player_database),))
            try:
                conn.execute("BEGIN IMMEDIATE")
                conn.execute(
                    "CREATE TABLE IF NOT EXISTS natal_effect_upgrade_operations ("
                    "operation_id TEXT PRIMARY KEY, user_id TEXT NOT NULL, scripture_id INTEGER NOT NULL, "
                    "scripture_cost INTEGER NOT NULL, max_slots INTEGER NOT NULL, max_effect_level INTEGER NOT NULL, "
                    "choice_seed INTEGER NOT NULL, slot INTEGER NOT NULL, effect_type INTEGER NOT NULL, "
                    "level INTEGER NOT NULL)"
                )
                previous = conn.execute(
                    "SELECT user_id, scripture_id, scripture_cost, max_slots, max_effect_level, choice_seed, "
                    "slot, effect_type, level FROM natal_effect_upgrade_operations WHERE operation_id=%s",
                    (operation_id,),
                ).fetchone()
                if previous is not None:
                    conn.rollback()
                    # Request identity = user + scripture cost knobs; seed/outcome in op row.
                    if str(previous[0]) != user_id or int(previous[1]) != scripture_id or int(previous[2]) != scripture_cost:
                        return result("state_changed")
                    return result("duplicate", *previous[6:])

                columns = {
                    str(row[1]) for row in conn.execute(
                        "PRAGMA player_data.table_info(natal_treasure)"
                    ).fetchall()
                }
                required = {"form"}
                for slot in range(1, max_slots + 1):
                    required.update({f"effect{slot}_type", f"effect{slot}_level"})
                if not required.issubset(columns):
                    conn.rollback()
                    return result("treasure_missing")
                fields = ["form"] + [
                    field for slot in range(1, max_slots + 1)
                    for field in (f"effect{slot}_type", f"effect{slot}_level")
                ]
                treasure = conn.execute(
                    "SELECT " + ", ".join(db_backend.quote_ident(field) for field in fields) +
                    " FROM player_data.natal_treasure WHERE user_id=%s", (user_id,),
                ).fetchone()
                if treasure is None or int(treasure[0] or 0) == 0:
                    conn.rollback()
                    return result("treasure_missing")
                candidates = []
                for slot in range(1, max_slots + 1):
                    effect_type = int(treasure[1 + (slot - 1) * 2] or 0)
                    level = int(treasure[2 + (slot - 1) * 2] or 0)
                    if effect_type > 0 and level < max_effect_level:
                        candidates.append((slot, effect_type, level))
                if not candidates:
                    conn.rollback()
                    return result("all_maxed")
                min_level = min(item[2] for item in candidates)
                lowest = [item for item in candidates if item[2] == min_level]
                slot, effect_type, level = random.Random(choice_seed).choice(lowest)
                item = conn.execute(
                    "SELECT COALESCE(goods_num, 0) FROM back WHERE user_id=%s AND goods_id=%s",
                    (user_id, scripture_id),
                ).fetchone()
                if item is None or int(item[0]) < scripture_cost:
                    conn.rollback()
                    return result("item_insufficient", slot, effect_type, level)
                consumed = conn.execute(
                    "UPDATE back SET goods_num=goods_num-%s WHERE user_id=%s AND goods_id=%s AND goods_num>=%s",
                    (scripture_cost, user_id, scripture_id, scripture_cost),
                )
                upgraded = conn.execute(
                    f"UPDATE player_data.natal_treasure SET {db_backend.quote_ident(f'effect{slot}_level')}=%s "
                    f"WHERE user_id=%s AND {db_backend.quote_ident(f'effect{slot}_level')}=%s",
                    (level + 1, user_id, level),
                )
                if consumed.rowcount != 1 or upgraded.rowcount != 1:
                    conn.rollback()
                    return result("state_changed")
                conn.execute(
                    "INSERT INTO natal_effect_upgrade_operations VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)",
                    (operation_id, user_id, scripture_id, scripture_cost, max_slots,
                     max_effect_level, choice_seed, slot, effect_type, level + 1),
                )
                conn.commit()
                return result("upgraded", slot, effect_type, level + 1)
            except Exception:
                conn.rollback()
                raise
            finally:
                conn.execute("DETACH DATABASE player_data")

@dataclass(frozen=True)
class EngravingResult:
    status: str
    slot: int
    effect_type: int
    base_value: float

    @property
    def succeeded(self) -> bool:
        return self.status in {"engraved", "duplicate"}

class EngravingService:
    """Consume a scripture and engrave a distinct effect atomically."""

    def __init__(self, game_database: str | Path, player_database: str | Path,
                 lock: RLock | None = None) -> None:
        self._game_database = Path(game_database)
        self._player_database = Path(player_database)
        self._lock = lock or RLock()

    def get_result(self, operation_id: str) -> EngravingResult | None:
        operation_id = str(operation_id).strip()
        if not operation_id:
            return None
        with self._lock, closing(db_backend.connect(self._game_database)) as conn:
            conn.execute(
                "CREATE TABLE IF NOT EXISTS natal_engraving_operations ("
                "operation_id TEXT PRIMARY KEY, user_id TEXT NOT NULL, scripture_id INTEGER NOT NULL, "
                "scripture_cost INTEGER NOT NULL, max_slots INTEGER NOT NULL, choice_seed INTEGER NOT NULL, "
                "slot INTEGER NOT NULL, effect_type INTEGER NOT NULL, base_value REAL NOT NULL)"
            )
            previous = conn.execute(
                "SELECT slot, effect_type, base_value FROM natal_engraving_operations WHERE operation_id=%s",
                (operation_id,),
            ).fetchone()
            if previous is None:
                return None
            return EngravingResult("duplicate", int(previous[0]), int(previous[1]), float(previous[2]))

    def engrave(self, operation_id, user_id, scripture_id, scripture_cost,
                max_slots, effect_configs, fixed_base_effects, choice_seed) -> EngravingResult:
        operation_id = str(operation_id).strip()
        user_id = str(user_id)
        scripture_id = int(scripture_id)
        scripture_cost = int(scripture_cost)
        max_slots = int(max_slots)
        choice_seed = int(choice_seed)
        configs = {
            int(key): (float(value[0]), float(value[1]))
            for key, value in effect_configs.items()
        }
        fixed = {int(value) for value in fixed_base_effects}
        if not operation_id or scripture_cost <= 0 or max_slots <= 0 or not configs:
            raise ValueError("valid operation, positive cost, slots and effect configs are required")

        def result(status, slot=0, effect_type=0, base_value=0.0):
            return EngravingResult(status, int(slot), int(effect_type), float(base_value))

        with self._lock, closing(db_backend.connect(self._game_database)) as conn:
            conn.execute("ATTACH DATABASE %s AS player_data", (str(self._player_database),))
            try:
                conn.execute("BEGIN IMMEDIATE")
                conn.execute(
                    "CREATE TABLE IF NOT EXISTS natal_engraving_operations ("
                    "operation_id TEXT PRIMARY KEY, user_id TEXT NOT NULL, scripture_id INTEGER NOT NULL, "
                    "scripture_cost INTEGER NOT NULL, max_slots INTEGER NOT NULL, choice_seed INTEGER NOT NULL, "
                    "slot INTEGER NOT NULL, effect_type INTEGER NOT NULL, base_value REAL NOT NULL)"
                )
                previous = conn.execute(
                    "SELECT user_id, scripture_id, scripture_cost, max_slots, choice_seed, slot, effect_type, "
                    "base_value FROM natal_engraving_operations WHERE operation_id=%s", (operation_id,),
                ).fetchone()
                if previous is not None:
                    conn.rollback()
                    if str(previous[0]) != user_id or int(previous[1]) != scripture_id or int(previous[2]) != scripture_cost:
                        return result("state_changed")
                    return result("duplicate", *previous[5:])

                columns = {
                    str(row[1]) for row in conn.execute(
                        "PRAGMA player_data.table_info(natal_treasure)"
                    ).fetchall()
                }
                required = {"form"}
                for slot in range(1, max_slots + 1):
                    required.update({f"effect{slot}_type", f"effect{slot}_base_value", f"effect{slot}_level"})
                if not required.issubset(columns):
                    conn.rollback()
                    return result("treasure_missing")
                type_fields = [f"effect{slot}_type" for slot in range(1, max_slots + 1)]
                treasure = conn.execute(
                    "SELECT form, " + ", ".join(db_backend.quote_ident(field) for field in type_fields) +
                    " FROM player_data.natal_treasure WHERE user_id=%s", (user_id,),
                ).fetchone()
                if treasure is None or int(treasure[0] or 0) == 0:
                    conn.rollback()
                    return result("treasure_missing")
                existing = {int(value) for value in treasure[1:] if int(value or 0) > 0}
                empty_slots = [index for index, value in enumerate(treasure[1:], 1) if int(value or 0) == 0]
                if not empty_slots:
                    conn.rollback()
                    return result("slots_full")
                available = sorted(set(configs) - existing)
                if not available:
                    conn.rollback()
                    return result("effect_exhausted")
                rng = random.Random(choice_seed)
                slot = empty_slots[0]
                effect_type = rng.choice(available)
                minimum, maximum = configs[effect_type]
                base_value = minimum if effect_type in fixed else round(rng.uniform(minimum, maximum), 3)
                item = conn.execute(
                    "SELECT COALESCE(goods_num, 0) FROM back WHERE user_id=%s AND goods_id=%s",
                    (user_id, scripture_id),
                ).fetchone()
                if item is None or int(item[0]) < scripture_cost:
                    conn.rollback()
                    return result("item_insufficient", slot, effect_type, base_value)
                consumed = conn.execute(
                    "UPDATE back SET goods_num=CAST(goods_num AS INTEGER)-%s "
                    "WHERE user_id=%s AND goods_id=%s AND CAST(COALESCE(goods_num,0) AS INTEGER)>=%s",
                    (scripture_cost, user_id, scripture_id, scripture_cost),
                )
                type_field = db_backend.quote_ident(f"effect{slot}_type")
                base_field = db_backend.quote_ident(f"effect{slot}_base_value")
                level_field = db_backend.quote_ident(f"effect{slot}_level")
                engraved = conn.execute(
                    f"UPDATE player_data.natal_treasure SET "
                    f"{type_field}=%s, {base_field}=%s, {level_field}=1 "
                    f"WHERE user_id=%s AND CAST(COALESCE({type_field}, 0) AS INTEGER)=0",
                    (effect_type, base_value, user_id),
                )
                if consumed.rowcount != 1 or engraved.rowcount != 1:
                    conn.rollback()
                    return result("state_changed")
                conn.execute(
                    "INSERT INTO natal_engraving_operations VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)",
                    (operation_id, user_id, scripture_id, scripture_cost, max_slots,
                     choice_seed, slot, effect_type, base_value),
                )
                conn.commit()
                return result("engraved", slot, effect_type, base_value)
            except Exception:
                conn.rollback()
                raise
            finally:
                conn.execute("DETACH DATABASE player_data")

@dataclass(frozen=True)
class ForgetEffectResult:
    status: str
    slot: int
    effect_type: int
    effect_level: int
    scripture_change: int

    @property
    def succeeded(self) -> bool:
        return self.status in {"forgotten", "duplicate"}

class ForgetEffectService:
    """Forget one effect and settle its scripture change atomically."""

    def __init__(self, game_database: str | Path, player_database: str | Path,
                 lock: RLock | None = None) -> None:
        self._game_database = Path(game_database)
        self._player_database = Path(player_database)
        self._lock = lock or RLock()

    def get_result(self, operation_id: str) -> ForgetEffectResult | None:
        operation_id = str(operation_id).strip()
        if not operation_id:
            return None
        with self._lock, closing(db_backend.connect(self._game_database)) as conn:
            conn.execute(
                "CREATE TABLE IF NOT EXISTS natal_forget_operations ("
                "operation_id TEXT PRIMARY KEY, user_id TEXT NOT NULL, effect_type INTEGER NOT NULL, "
                "scripture_id INTEGER NOT NULL, scripture_cost INTEGER NOT NULL, max_slots INTEGER NOT NULL, "
                "slot INTEGER NOT NULL, effect_level INTEGER NOT NULL, scripture_change INTEGER NOT NULL)"
            )
            previous = conn.execute(
                "SELECT effect_type, slot, effect_level, scripture_change FROM natal_forget_operations WHERE operation_id=%s",
                (operation_id,),
            ).fetchone()
            if previous is None:
                return None
            return ForgetEffectResult("duplicate", int(previous[1]), int(previous[0]), int(previous[2]), int(previous[3]))

    def forget(self, operation_id, user_id, effect_type, scripture_id,
               scripture_name, scripture_type, scripture_cost,
               max_slots, max_goods_num) -> ForgetEffectResult:
        operation_id = str(operation_id).strip()
        user_id = str(user_id)
        effect_type = int(effect_type)
        scripture_id = int(scripture_id)
        scripture_name = str(scripture_name)
        scripture_type = str(scripture_type)
        scripture_cost = int(scripture_cost)
        max_slots = int(max_slots)
        max_goods_num = int(max_goods_num)
        if (not operation_id or effect_type <= 0 or scripture_cost < 0
                or max_slots <= 1 or max_goods_num <= 0):
            raise ValueError("valid operation and forget parameters are required")

        def result(status, slot=0, level=0, change=0):
            return ForgetEffectResult(
                status, int(slot), effect_type, int(level), int(change),
            )

        with self._lock, closing(db_backend.connect(self._game_database)) as conn:
            conn.execute("ATTACH DATABASE %s AS player_data", (str(self._player_database),))
            try:
                conn.execute("BEGIN IMMEDIATE")
                conn.execute(
                    "CREATE TABLE IF NOT EXISTS natal_forget_operations ("
                    "operation_id TEXT PRIMARY KEY, user_id TEXT NOT NULL, effect_type INTEGER NOT NULL, "
                    "scripture_id INTEGER NOT NULL, scripture_cost INTEGER NOT NULL, max_slots INTEGER NOT NULL, "
                    "slot INTEGER NOT NULL, effect_level INTEGER NOT NULL, scripture_change INTEGER NOT NULL)"
                )
                previous = conn.execute(
                    "SELECT user_id, effect_type, scripture_id, scripture_cost, max_slots, slot, "
                    "effect_level, scripture_change FROM natal_forget_operations WHERE operation_id=%s",
                    (operation_id,),
                ).fetchone()
                if previous is not None:
                    conn.rollback()
                    if str(previous[0]) != user_id or int(previous[1]) != effect_type:
                        return result("state_changed")
                    return result("duplicate", previous[5], previous[6], previous[7])

                required = {"form"}
                fields = ["form"]
                for slot in range(1, max_slots + 1):
                    slot_fields = (
                        f"effect{slot}_type", f"effect{slot}_base_value", f"effect{slot}_level",
                    )
                    required.update(slot_fields)
                    fields.extend(slot_fields)
                columns = {
                    str(row[1]) for row in conn.execute(
                        "PRAGMA player_data.table_info(natal_treasure)"
                    ).fetchall()
                }
                if not required.issubset(columns):
                    conn.rollback()
                    return result("treasure_missing")
                treasure = conn.execute(
                    "SELECT " + ", ".join(db_backend.quote_ident(field) for field in fields)
                    + " FROM player_data.natal_treasure WHERE user_id=%s",
                    (user_id,),
                ).fetchone()
                if treasure is None or int(treasure[0] or 0) == 0:
                    conn.rollback()
                    return result("treasure_missing")

                occupied = 0
                target_slot = 0
                target_level = 0
                for slot in range(1, max_slots + 1):
                    offset = 1 + (slot - 1) * 3
                    current_type = int(treasure[offset] or 0)
                    if current_type > 0:
                        occupied += 1
                    if current_type == effect_type:
                        target_slot = slot
                        target_level = int(treasure[offset + 2] or 0)
                if target_slot == 0:
                    conn.rollback()
                    return result("effect_missing")
                if occupied <= 1:
                    conn.rollback()
                    return result("last_effect")

                scripture_change = max(0, target_level - 1) - scripture_cost
                item = conn.execute(
                    "SELECT COALESCE(goods_num, 0) FROM back WHERE user_id=%s AND goods_id=%s",
                    (user_id, scripture_id),
                ).fetchone()
                current_quantity = int(item[0] or 0) if item else 0
                if scripture_change < 0 and current_quantity < -scripture_change:
                    conn.rollback()
                    return result("item_insufficient", target_slot, target_level, scripture_change)
                if scripture_change > 0 and current_quantity + scripture_change > max_goods_num:
                    conn.rollback()
                    return result("inventory_full", target_slot, target_level, scripture_change)

                if scripture_change < 0:
                    changed = conn.execute(
                        "UPDATE back SET goods_num=goods_num-%s WHERE user_id=%s AND goods_id=%s "
                        "AND goods_num>=%s",
                        (-scripture_change, user_id, scripture_id, -scripture_change),
                    )
                    if changed.rowcount != 1:
                        conn.rollback()
                        return result("state_changed")
                elif scripture_change > 0:
                    if item is None:
                        back_columns = set(conn.column_names("back"))
                        insert_columns = ["user_id", "goods_id", "goods_name", "goods_type", "goods_num"]
                        insert_values = [user_id, scripture_id, scripture_name, scripture_type, scripture_change]
                        if "bind_num" in back_columns:
                            insert_columns.append("bind_num")
                            insert_values.append(0)
                        conn.execute(
                            "INSERT INTO back (" + ", ".join(
                                db_backend.quote_ident(column) for column in insert_columns
                            ) + ") VALUES (" + ", ".join("%s" for _ in insert_columns) + ")",
                            tuple(insert_values),
                        )
                    else:
                        changed = conn.execute(
                            "UPDATE back SET goods_num=goods_num+%s WHERE user_id=%s AND goods_id=%s "
                            "AND goods_num+%s<=%s",
                            (scripture_change, user_id, scripture_id, scripture_change, max_goods_num),
                        )
                        if changed.rowcount != 1:
                            conn.rollback()
                            return result("state_changed")

                cleared = conn.execute(
                    f"UPDATE player_data.natal_treasure SET "
                    f"{db_backend.quote_ident(f'effect{target_slot}_type')}=0, "
                    f"{db_backend.quote_ident(f'effect{target_slot}_base_value')}=0.0, "
                    f"{db_backend.quote_ident(f'effect{target_slot}_level')}=0 "
                    f"WHERE user_id=%s AND {db_backend.quote_ident(f'effect{target_slot}_type')}=%s "
                    f"AND {db_backend.quote_ident(f'effect{target_slot}_level')}=%s",
                    (user_id, effect_type, target_level),
                )
                if cleared.rowcount != 1:
                    conn.rollback()
                    return result("state_changed")
                conn.execute(
                    "INSERT INTO natal_forget_operations VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)",
                    (operation_id, user_id, effect_type, scripture_id, scripture_cost,
                     max_slots, target_slot, target_level, scripture_change),
                )
                conn.commit()
                return result("forgotten", target_slot, target_level, scripture_change)
            except Exception:
                conn.rollback()
                raise
            finally:
                conn.execute("DETACH DATABASE player_data")

@dataclass(frozen=True)
class ReawakenResult:
    status: str
    form: int
    name: str
    effect_type: int
    base_value: float
    scripture_change: int

    @property
    def succeeded(self) -> bool:
        return self.status in {"reawakened", "duplicate"}

class ReawakenService:
    """Reset a natal treasure and settle scripture refunds atomically."""

    def __init__(self, game_database: str | Path, player_database: str | Path,
                 lock: RLock | None = None) -> None:
        self._game_database = Path(game_database)
        self._player_database = Path(player_database)
        self._lock = lock or RLock()

    def get_result(self, operation_id: str) -> ReawakenResult | None:
        operation_id = str(operation_id).strip()
        if not operation_id:
            return None
        with self._lock, closing(db_backend.connect(self._game_database)) as conn:
            conn.execute(
                "CREATE TABLE IF NOT EXISTS natal_reawaken_operations ("
                "operation_id TEXT PRIMARY KEY, user_id TEXT NOT NULL, scripture_id INTEGER NOT NULL, "
                "scripture_cost INTEGER NOT NULL, max_slots INTEGER NOT NULL, choice_seed INTEGER NOT NULL, "
                "form INTEGER NOT NULL, "
                "name TEXT NOT NULL, effect_type INTEGER NOT NULL, base_value REAL NOT NULL, "
                "scripture_change INTEGER NOT NULL)"
            )
            previous = conn.execute(
                "SELECT form, name, effect_type, base_value, scripture_change FROM natal_reawaken_operations WHERE operation_id=%s",
                (operation_id,),
            ).fetchone()
            if previous is None:
                return None
            return ReawakenResult("duplicate", int(previous[0]), str(previous[1]), int(previous[2]), float(previous[3]), int(previous[4]))

    def reawaken(self, operation_id, user_id, scripture_id, scripture_name,
                 scripture_type, scripture_cost, max_slots, max_goods_num,
                 effect_configs, effect_names, fixed_base_effects,
                 choice_seed) -> ReawakenResult:
        operation_id = str(operation_id).strip()
        user_id = str(user_id)
        scripture_id = int(scripture_id)
        scripture_name = str(scripture_name)
        scripture_type = str(scripture_type)
        scripture_cost = int(scripture_cost)
        max_slots = int(max_slots)
        max_goods_num = int(max_goods_num)
        choice_seed = int(choice_seed)
        configs = {
            int(key): (float(value[0]), float(value[1]))
            for key, value in effect_configs.items()
        }
        names = {
            int(key): tuple(str(name) for name in value)
            for key, value in effect_names.items()
        }
        fixed = {int(value) for value in fixed_base_effects}
        if (not operation_id or scripture_cost < 0 or max_slots <= 0
                or max_goods_num <= 0 or not configs
                or any(not names.get(effect_type) for effect_type in configs)):
            raise ValueError("valid operation and reawaken parameters are required")

        def result(status, form=0, name="", effect_type=0,
                   base_value=0.0, change=0):
            return ReawakenResult(
                status, int(form), str(name), int(effect_type),
                float(base_value), int(change),
            )

        with self._lock, closing(db_backend.connect(self._game_database)) as conn:
            conn.execute("ATTACH DATABASE %s AS player_data", (str(self._player_database),))
            try:
                conn.execute("BEGIN IMMEDIATE")
                conn.execute(
                    "CREATE TABLE IF NOT EXISTS natal_reawaken_operations ("
                    "operation_id TEXT PRIMARY KEY, user_id TEXT NOT NULL, scripture_id INTEGER NOT NULL, "
                    "scripture_cost INTEGER NOT NULL, max_slots INTEGER NOT NULL, choice_seed INTEGER NOT NULL, "
                    "form INTEGER NOT NULL, "
                    "name TEXT NOT NULL, effect_type INTEGER NOT NULL, base_value REAL NOT NULL, "
                    "scripture_change INTEGER NOT NULL)"
                )
                previous = conn.execute(
                    "SELECT user_id, scripture_id, scripture_cost, max_slots, choice_seed, form, name, effect_type, "
                    "base_value, scripture_change FROM natal_reawaken_operations WHERE operation_id=%s",
                    (operation_id,),
                ).fetchone()
                if previous is not None:
                    conn.rollback()
                    if str(previous[0]) != user_id or int(previous[1]) != scripture_id or int(previous[2]) != scripture_cost:
                        return result("state_changed")
                    return result("duplicate", *previous[5:])

                required = {
                    "form", "name", "level", "exp", "max_exp",
                    "fate_revive_count", "immortal_revive_count",
                    "invincible_gain_count", "nirvana_revive_count",
                    "soul_return_revive_count", "charge_status",
                    "soul_summon_count", "enlightenment_count",
                }
                fields = ["form"]
                for slot in range(1, max_slots + 1):
                    slot_fields = (
                        f"effect{slot}_type", f"effect{slot}_base_value", f"effect{slot}_level",
                    )
                    required.update(slot_fields)
                    fields.append(f"effect{slot}_level")
                columns = {
                    str(row[1]) for row in conn.execute(
                        "PRAGMA player_data.table_info(natal_treasure)"
                    ).fetchall()
                }
                if not required.issubset(columns):
                    conn.rollback()
                    return result("treasure_missing")
                treasure = conn.execute(
                    "SELECT " + ", ".join(db_backend.quote_ident(field) for field in fields)
                    + " FROM player_data.natal_treasure WHERE user_id=%s",
                    (user_id,),
                ).fetchone()
                if treasure is None or int(treasure[0] or 0) == 0:
                    conn.rollback()
                    return result("treasure_missing")

                refund = sum(max(0, int(level or 0) - 1) for level in treasure[1:])
                scripture_change = refund - scripture_cost
                item = conn.execute(
                    "SELECT COALESCE(goods_num, 0) FROM back WHERE user_id=%s AND goods_id=%s",
                    (user_id, scripture_id),
                ).fetchone()
                current_quantity = int(item[0] or 0) if item else 0
                if scripture_change < 0 and current_quantity < -scripture_change:
                    conn.rollback()
                    return result("item_insufficient", change=scripture_change)
                if scripture_change > 0 and current_quantity + scripture_change > max_goods_num:
                    conn.rollback()
                    return result("inventory_full", change=scripture_change)

                rng = random.Random(choice_seed)
                form = rng.randint(1, 4)
                effect_type = rng.choice(sorted(configs))
                name = rng.choice(names[effect_type])
                minimum, maximum = configs[effect_type]
                base_value = minimum if effect_type in fixed else round(rng.uniform(minimum, maximum), 3)

                if scripture_change < 0:
                    changed = conn.execute(
                        "UPDATE back SET goods_num=goods_num-%s WHERE user_id=%s AND goods_id=%s "
                        "AND goods_num>=%s",
                        (-scripture_change, user_id, scripture_id, -scripture_change),
                    )
                    if changed.rowcount != 1:
                        conn.rollback()
                        return result("state_changed")
                elif scripture_change > 0:
                    if item is None:
                        back_columns = set(conn.column_names("back"))
                        insert_columns = ["user_id", "goods_id", "goods_name", "goods_type", "goods_num"]
                        insert_values = [user_id, scripture_id, scripture_name, scripture_type, scripture_change]
                        if "bind_num" in back_columns:
                            insert_columns.append("bind_num")
                            insert_values.append(0)
                        conn.execute(
                            "INSERT INTO back (" + ", ".join(
                                db_backend.quote_ident(column) for column in insert_columns
                            ) + ") VALUES (" + ", ".join("%s" for _ in insert_columns) + ")",
                            tuple(insert_values),
                        )
                    else:
                        changed = conn.execute(
                            "UPDATE back SET goods_num=goods_num+%s WHERE user_id=%s AND goods_id=%s "
                            "AND goods_num+%s<=%s",
                            (scripture_change, user_id, scripture_id, scripture_change, max_goods_num),
                        )
                        if changed.rowcount != 1:
                            conn.rollback()
                            return result("state_changed")

                assignments = [
                    ("form", form), ("name", name), ("level", 0),
                    ("exp", 0), ("max_exp", 100),
                    ("fate_revive_count", 0), ("immortal_revive_count", 0),
                    ("invincible_gain_count", 0), ("nirvana_revive_count", 0),
                    ("soul_return_revive_count", 0), ("charge_status", 0),
                    ("soul_summon_count", "{}"), ("enlightenment_count", "{}"),
                ]
                for slot in range(1, max_slots + 1):
                    assignments.extend((
                        (f"effect{slot}_type", effect_type if slot == 1 else 0),
                        (f"effect{slot}_base_value", base_value if slot == 1 else 0.0),
                        (f"effect{slot}_level", 1 if slot == 1 else 0),
                    ))
                updated = conn.execute(
                    "UPDATE player_data.natal_treasure SET " + ", ".join(
                        f"{db_backend.quote_ident(field)}=%s" for field, _ in assignments
                    ) + " WHERE user_id=%s AND form=%s",
                    tuple(value for _, value in assignments) + (user_id, int(treasure[0])),
                )
                if updated.rowcount != 1:
                    conn.rollback()
                    return result("state_changed")
                conn.execute(
                    "INSERT INTO natal_reawaken_operations VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)",
                    (operation_id, user_id, scripture_id, scripture_cost, max_slots,
                     choice_seed, form, name, effect_type, base_value, scripture_change),
                )
                conn.commit()
                return result("reawakened", form, name, effect_type, base_value, scripture_change)
            except Exception:
                conn.rollback()
                raise
            finally:
                conn.execute("DETACH DATABASE player_data")

@dataclass(frozen=True)
class AwakenResult:
    status: str
    form: int
    name: str
    effect_type: int
    base_value: float

    @property
    def succeeded(self) -> bool:
        return self.status in {"awakened", "duplicate"}

class AwakenService:
    """Create the first natal treasure state atomically and idempotently."""

    def __init__(self, player_database: str | Path,
                 lock: RLock | None = None) -> None:
        self._player_database = Path(player_database)
        self._lock = lock or RLock()

    def get_result(self, operation_id: str) -> AwakenResult | None:
        operation_id = str(operation_id).strip()
        if not operation_id:
            return None
        with self._lock, closing(db_backend.connect(self._player_database)) as conn:
            conn.execute(
                "CREATE TABLE IF NOT EXISTS natal_awaken_operations ("
                "operation_id TEXT PRIMARY KEY, user_id TEXT NOT NULL, max_slots INTEGER NOT NULL, "
                "choice_seed INTEGER NOT NULL, form INTEGER NOT NULL, name TEXT NOT NULL, "
                "effect_type INTEGER NOT NULL, base_value REAL NOT NULL)"
            )
            previous = conn.execute(
                "SELECT form, name, effect_type, base_value FROM natal_awaken_operations WHERE operation_id=%s",
                (operation_id,),
            ).fetchone()
            if previous is None:
                return None
            return AwakenResult("duplicate", int(previous[0]), str(previous[1]), int(previous[2]), float(previous[3]))

    def awaken(self, operation_id, user_id, max_slots, effect_configs,
               effect_names, fixed_base_effects, choice_seed) -> AwakenResult:
        operation_id = str(operation_id).strip()
        user_id = str(user_id)
        max_slots = int(max_slots)
        choice_seed = int(choice_seed)
        configs = {
            int(key): (float(value[0]), float(value[1]))
            for key, value in effect_configs.items()
        }
        names = {
            int(key): tuple(str(name) for name in value)
            for key, value in effect_names.items()
        }
        fixed = {int(value) for value in fixed_base_effects}
        if (not operation_id or max_slots <= 0 or not configs
                or any(not names.get(effect_type) for effect_type in configs)):
            raise ValueError("valid operation and awaken parameters are required")

        def result(status, form=0, name="", effect_type=0, base_value=0.0):
            return AwakenResult(
                status, int(form), str(name), int(effect_type), float(base_value),
            )

        with self._lock, closing(db_backend.connect(self._player_database)) as conn:
            try:
                conn.execute("BEGIN IMMEDIATE")
                conn.execute(
                    "CREATE TABLE IF NOT EXISTS natal_awaken_operations ("
                    "operation_id TEXT PRIMARY KEY, user_id TEXT NOT NULL, max_slots INTEGER NOT NULL, "
                    "choice_seed INTEGER NOT NULL, form INTEGER NOT NULL, name TEXT NOT NULL, "
                    "effect_type INTEGER NOT NULL, base_value REAL NOT NULL)"
                )
                previous = conn.execute(
                    "SELECT user_id, max_slots, choice_seed, form, name, effect_type, base_value "
                    "FROM natal_awaken_operations WHERE operation_id=%s",
                    (operation_id,),
                ).fetchone()
                if previous is not None:
                    conn.rollback()
                    # Request identity = user_id only; seed/outcome stored in op row.
                    if str(previous[0]) != user_id:
                        return result("state_changed")
                    return result("duplicate", *previous[3:])

                required = {
                    "form", "name", "level", "exp", "max_exp",
                    "fate_revive_count", "immortal_revive_count",
                    "invincible_gain_count", "nirvana_revive_count",
                    "soul_return_revive_count", "charge_status",
                    "soul_summon_count", "enlightenment_count",
                }
                for slot in range(1, max_slots + 1):
                    required.update((
                        f"effect{slot}_type", f"effect{slot}_base_value", f"effect{slot}_level",
                    ))
                if not conn.table_exists("natal_treasure"):
                    conn.rollback()
                    return result("treasure_missing")
                columns = set(conn.column_names("natal_treasure"))
                if not required.issubset(columns):
                    conn.rollback()
                    return result("treasure_missing")
                current = conn.execute(
                    "SELECT form FROM natal_treasure WHERE user_id=%s", (user_id,),
                ).fetchone()
                if current is None:
                    conn.rollback()
                    return result("treasure_missing")
                if int(current[0] or 0) != 0:
                    conn.rollback()
                    return result("already_awakened")

                rng = random.Random(choice_seed)
                form = rng.randint(1, 4)
                effect_type = rng.choice(sorted(configs))
                name = rng.choice(names[effect_type])
                minimum, maximum = configs[effect_type]
                base_value = minimum if effect_type in fixed else round(rng.uniform(minimum, maximum), 3)
                assignments = [
                    ("form", form), ("name", name), ("level", 0),
                    ("exp", 0), ("max_exp", 100),
                    ("fate_revive_count", 0), ("immortal_revive_count", 0),
                    ("invincible_gain_count", 0), ("nirvana_revive_count", 0),
                    ("soul_return_revive_count", 0), ("charge_status", 0),
                    ("soul_summon_count", "{}"), ("enlightenment_count", "{}"),
                ]
                for slot in range(1, max_slots + 1):
                    assignments.extend((
                        (f"effect{slot}_type", effect_type if slot == 1 else 0),
                        (f"effect{slot}_base_value", base_value if slot == 1 else 0.0),
                        (f"effect{slot}_level", 1 if slot == 1 else 0),
                    ))
                updated = conn.execute(
                    "UPDATE natal_treasure SET " + ", ".join(
                        f"{db_backend.quote_ident(field)}=%s" for field, _ in assignments
                    ) + " WHERE user_id=%s AND COALESCE(form, 0)=0",
                    tuple(value for _, value in assignments) + (user_id,),
                )
                if updated.rowcount != 1:
                    conn.rollback()
                    return result("state_changed")
                conn.execute(
                    "INSERT INTO natal_awaken_operations VALUES (%s, %s, %s, %s, %s, %s, %s, %s)",
                    (operation_id, user_id, max_slots, choice_seed,
                     form, name, effect_type, base_value),
                )
                conn.commit()
                return result("awakened", form, name, effect_type, base_value)
            except Exception:
                conn.rollback()
                raise

__all__ = [
    "NatalTrainingResult",
    "NatalTrainingService",
    "EffectUpgradeResult",
    "EffectUpgradeService",
    "EngravingResult",
    "EngravingService",
    "ForgetEffectResult",
    "ForgetEffectService",
    "ReawakenResult",
    "ReawakenService",
    "AwakenResult",
    "AwakenService",
]
