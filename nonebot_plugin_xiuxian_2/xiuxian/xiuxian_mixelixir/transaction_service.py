from __future__ import annotations

import json
from contextlib import closing
from dataclasses import dataclass, field
from pathlib import Path
from threading import RLock
from ..xiuxian_utils import db_backend

@dataclass(frozen=True)
class MixelixirRecipeSaveResult:
    status: str
    recipe_set_id: str
    recipes: tuple[dict, ...]

    @property
    def succeeded(self) -> bool:
        return self.status in {"applied", "duplicate"}

class MixelixirRecipeService:
    """Atomically replace a player's generated recipes from an inventory snapshot."""

    def __init__(self, database: str | Path, lock: RLock | None = None) -> None:
        self._database = Path(database)
        self._lock = lock or RLock()

    @staticmethod
    def _normalize_inventory(items) -> tuple[tuple[int, str, int], ...]:
        normalized = []
        for item in items:
            item_id, name, quantity = int(item["id"]), str(item["name"]), int(item["quantity"])
            if quantity > 0:
                normalized.append((item_id, name, quantity))
        return tuple(sorted(normalized))

    @staticmethod
    def _decode_recipes(value: str) -> tuple[dict, ...]:
        return tuple(dict(recipe) for recipe in json.loads(value))

    def get_result(self, operation_id: str) -> MixelixirRecipeSaveResult | None:
        operation_id = str(operation_id).strip()
        if not operation_id:
            return None
        with self._lock, closing(db_backend.connect(self._database)) as conn:
            conn.execute(
                "CREATE TABLE IF NOT EXISTS mixelixir_recipe_save_operations ("
                "operation_id TEXT PRIMARY KEY, payload TEXT NOT NULL, recipe_set_id TEXT NOT NULL, "
                "recipes_json TEXT NOT NULL, created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP)"
            )
            previous = conn.execute(
                "SELECT payload,recipe_set_id,recipes_json FROM mixelixir_recipe_save_operations WHERE operation_id=%s",
                (operation_id,),
            ).fetchone()
            if previous is None:
                return None
            return MixelixirRecipeSaveResult("duplicate", str(previous[1]), self._decode_recipes(str(previous[2])))

    def save(
        self,
        operation_id,
        user_id,
        expected_daily_count,
        materials,
        furnaces,
        recipes,
    ) -> MixelixirRecipeSaveResult:
        operation_id, user_id = str(operation_id).strip(), str(user_id)
        expected_daily_count = int(expected_daily_count)
        material_snapshot = self._normalize_inventory(materials)
        furnace_snapshot = self._normalize_inventory(furnaces)
        normalized_recipes = tuple(dict(recipe) for recipe in recipes)
        if not operation_id or expected_daily_count < 0 or not material_snapshot or not furnace_snapshot or not normalized_recipes:
            raise ValueError("valid operation, inventory snapshot and recipes are required")
        recipes_json = json.dumps(normalized_recipes, ensure_ascii=False, sort_keys=True)
        # Request identity only — inventory/recipes are snapshot+roll outcomes.
        payload = json.dumps([user_id], ensure_ascii=True, separators=(",", ":"))
        recipe_set_id = operation_id

        with self._lock, closing(db_backend.connect(self._database)) as conn:
            try:
                conn.execute("BEGIN IMMEDIATE")
                conn.execute(
                    "CREATE TABLE IF NOT EXISTS mixelixir_recipe_sets ("
                    "user_id TEXT PRIMARY KEY, recipe_set_id TEXT UNIQUE NOT NULL, daily_count INTEGER NOT NULL, "
                    "materials_json TEXT NOT NULL, furnaces_json TEXT NOT NULL, recipes_json TEXT NOT NULL, "
                    "created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP)"
                )
                conn.execute(
                    "CREATE TABLE IF NOT EXISTS mixelixir_recipe_save_operations ("
                    "operation_id TEXT PRIMARY KEY, payload TEXT NOT NULL, recipe_set_id TEXT NOT NULL, "
                    "recipes_json TEXT NOT NULL, created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP)"
                )
                previous = conn.execute(
                    "SELECT payload,recipe_set_id,recipes_json FROM mixelixir_recipe_save_operations WHERE operation_id=%s",
                    (operation_id,),
                ).fetchone()
                if previous is not None:
                    conn.rollback()
                    if str(previous[0]) != payload:
                        return MixelixirRecipeSaveResult("state_changed", "", ())
                    return MixelixirRecipeSaveResult("duplicate", str(previous[1]), self._decode_recipes(str(previous[2])))

                user = conn.execute(
                    "SELECT COALESCE(mixelixir_num,0) FROM user_xiuxian WHERE user_id=%s", (user_id,)
                ).fetchone()
                if user is None:
                    conn.rollback()
                    return MixelixirRecipeSaveResult("user_missing", "", ())
                if int(user[0]) != expected_daily_count:
                    conn.rollback()
                    return MixelixirRecipeSaveResult("state_changed", "", ())

                current_materials = self._normalize_inventory(
                    {"id": row[0], "name": row[1], "quantity": row[2]}
                    for row in conn.execute(
                        "SELECT goods_id,goods_name,COALESCE(goods_num,0) FROM back "
                        "WHERE user_id=%s AND goods_type=%s AND COALESCE(goods_num,0)>0",
                        (user_id, "药材"),
                    ).fetchall()
                )
                current_furnaces = self._normalize_inventory(
                    {"id": row[0], "name": row[1], "quantity": row[2]}
                    for row in conn.execute(
                        "SELECT goods_id,goods_name,COALESCE(goods_num,0) FROM back "
                        "WHERE user_id=%s AND goods_type=%s AND COALESCE(goods_num,0)>0",
                        (user_id, "炼丹炉"),
                    ).fetchall()
                )
                if current_materials != material_snapshot or current_furnaces != furnace_snapshot:
                    conn.rollback()
                    return MixelixirRecipeSaveResult("state_changed", "", ())

                conn.execute(
                    "INSERT INTO mixelixir_recipe_sets "
                    "(user_id,recipe_set_id,daily_count,materials_json,furnaces_json,recipes_json) "
                    "VALUES (%s,%s,%s,%s,%s,%s) ON CONFLICT(user_id) DO UPDATE SET "
                    "recipe_set_id=EXCLUDED.recipe_set_id,daily_count=EXCLUDED.daily_count,"
                    "materials_json=EXCLUDED.materials_json,furnaces_json=EXCLUDED.furnaces_json,"
                    "recipes_json=EXCLUDED.recipes_json,created_at=CURRENT_TIMESTAMP",
                    (
                        user_id,
                        recipe_set_id,
                        expected_daily_count,
                        json.dumps(material_snapshot, ensure_ascii=False),
                        json.dumps(furnace_snapshot, ensure_ascii=False),
                        recipes_json,
                    ),
                )
                conn.execute(
                    "INSERT INTO mixelixir_recipe_save_operations "
                    "(operation_id,payload,recipe_set_id,recipes_json) VALUES (%s,%s,%s,%s)",
                    (operation_id, payload, recipe_set_id, recipes_json),
                )
                conn.commit()
                return MixelixirRecipeSaveResult("applied", recipe_set_id, normalized_recipes)
            except Exception:
                conn.rollback()
                raise

    def find(self, user_id, submitted_recipe) -> tuple[str, dict] | None:
        user_id = str(user_id)
        submitted = "".join(str(submitted_recipe).split())
        if submitted.startswith("配方"):
            submitted = submitted[2:]
        with self._lock, closing(db_backend.connect(self._database)) as conn:
            row = conn.execute(
                "SELECT recipe_set_id,recipes_json FROM mixelixir_recipe_sets WHERE user_id=%s", (user_id,)
            ).fetchone()
        if row is None:
            return None
        for recipe in self._decode_recipes(str(row[1])):
            if str(recipe.get("recipe_key", "")) == submitted:
                return str(row[0]), recipe
        return None

@dataclass(frozen=True)
class HarvestReward:
    item_id: int
    name: str
    quantity: int

@dataclass(frozen=True)
class MixelixirHarvestResult:
    status: str
    harvested_at: str
    rewards: tuple[HarvestReward, ...]

    @property
    def succeeded(self) -> bool:
        return self.status in {"applied", "duplicate"}

class MixelixirHarvestService:
    """Grant field rewards and advance harvest time atomically across databases."""

    def __init__(self, game_database: str | Path, player_database: str | Path, lock: RLock | None = None) -> None:
        self._game_database = Path(game_database)
        self._player_database = Path(player_database)
        self._lock = lock or RLock()

    @staticmethod
    def _decode_rewards(payload: str) -> tuple[HarvestReward, ...]:
        return tuple(HarvestReward(**reward) for reward in json.loads(payload))

    def get_result(self, operation_id: str) -> MixelixirHarvestResult | None:
        operation_id = str(operation_id).strip()
        if not operation_id:
            return None
        with self._lock, closing(db_backend.connect(self._game_database)) as conn:
            conn.execute(
                "CREATE TABLE IF NOT EXISTS mixelixir_harvest_operations ("
                "operation_id TEXT PRIMARY KEY, payload TEXT NOT NULL, harvested_at TEXT NOT NULL, "
                "rewards_json TEXT NOT NULL, created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP)"
            )
            previous = conn.execute(
                "SELECT payload, harvested_at, rewards_json FROM mixelixir_harvest_operations WHERE operation_id=%s",
                (operation_id,),
            ).fetchone()
            if previous is None:
                return None
            return MixelixirHarvestResult("duplicate", str(previous[1]), self._decode_rewards(str(previous[2])))

    def harvest(
        self,
        operation_id,
        user_id,
        expected_last_time,
        harvested_at,
        rewards,
        *,
        max_goods_num,
    ) -> MixelixirHarvestResult:
        operation_id = str(operation_id).strip()
        user_id = str(user_id)
        expected_last_time = str(expected_last_time)
        harvested_at = str(harvested_at)
        max_goods_num = int(max_goods_num)
        merged: dict[int, HarvestReward] = {}
        for reward in rewards:
            normalized = reward if isinstance(reward, HarvestReward) else HarvestReward(
                int(reward[0]), str(reward[1]), int(reward[2])
            )
            if normalized.quantity <= 0:
                continue
            previous = merged.get(normalized.item_id)
            quantity = normalized.quantity + (previous.quantity if previous else 0)
            merged[normalized.item_id] = HarvestReward(normalized.item_id, normalized.name, quantity)
        normalized_rewards = tuple(sorted(merged.values(), key=lambda reward: reward.item_id))
        if not operation_id or not harvested_at or not normalized_rewards or max_goods_num <= 0:
            raise ValueError("operation, harvest time, rewards and capacity are required")

        rewards_json = json.dumps(
            [reward.__dict__ for reward in normalized_rewards], ensure_ascii=True, sort_keys=True
        )
        # Request identity only — harvest time/rewards are outcomes stored in op row.
        payload = json.dumps([user_id], ensure_ascii=True, separators=(",", ":"))
        with self._lock, closing(db_backend.connect(self._game_database)) as conn:
            attached = False
            try:
                conn.execute("ATTACH DATABASE %s AS player_data", (str(self._player_database),))
                attached = True
                conn.execute("BEGIN IMMEDIATE")
                conn.execute(
                    "CREATE TABLE IF NOT EXISTS mixelixir_harvest_operations ("
                    "operation_id TEXT PRIMARY KEY, payload TEXT NOT NULL, harvested_at TEXT NOT NULL, "
                    "rewards_json TEXT NOT NULL, created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP)"
                )
                previous = conn.execute(
                    "SELECT payload, harvested_at, rewards_json FROM mixelixir_harvest_operations WHERE operation_id=%s",
                    (operation_id,),
                ).fetchone()
                if previous is not None:
                    conn.rollback()
                    if str(previous[0]) != payload:
                        return MixelixirHarvestResult("state_changed", harvested_at, ())
                    return MixelixirHarvestResult("duplicate", str(previous[1]), self._decode_rewards(str(previous[2])))

                user = conn.execute("SELECT 1 FROM user_xiuxian WHERE user_id=%s", (user_id,)).fetchone()
                if user is None:
                    conn.rollback()
                    return MixelixirHarvestResult("user_missing", harvested_at, ())
                table = conn.execute(
                    "SELECT 1 FROM player_data.sqlite_master WHERE type='table' AND name=%s",
                    ("mix_elixir_info",),
                ).fetchone()
                if table is None:
                    conn.rollback()
                    return MixelixirHarvestResult("state_changed", harvested_at, ())
                columns = {
                    str(column[1])
                    for column in conn.execute("PRAGMA player_data.table_info(mix_elixir_info)").fetchall()
                }
                if "收取时间" not in columns:
                    conn.rollback()
                    return MixelixirHarvestResult("state_changed", harvested_at, ())
                row = conn.execute(
                    f"SELECT {db_backend.quote_ident('收取时间')} FROM player_data.mix_elixir_info WHERE user_id=%s",
                    (user_id,),
                ).fetchone()
                if row is None or str(row[0]) != expected_last_time:
                    conn.rollback()
                    return MixelixirHarvestResult("state_changed", harvested_at, ())

                back_columns = set(conn.column_names("back"))
                insert_columns = "user_id, goods_id, goods_name, goods_type, goods_num"
                insert_values = "%s, %s, %s, %s, %s"
                if "bind_num" in back_columns:
                    insert_columns += ", bind_num"
                    insert_values += ", 0"
                for reward in normalized_rewards:
                    conn.execute(
                        f"INSERT INTO back ({insert_columns}) VALUES ({insert_values}) "
                        "ON CONFLICT (user_id, goods_id) DO UPDATE SET "
                        "goods_name=EXCLUDED.goods_name, goods_type=EXCLUDED.goods_type, "
                        "goods_num=MIN(COALESCE(back.goods_num, 0)+EXCLUDED.goods_num, %s)",
                        (user_id, reward.item_id, reward.name, "药材", reward.quantity, max_goods_num),
                    )
                updated = conn.execute(
                    f"UPDATE player_data.mix_elixir_info SET {db_backend.quote_ident('收取时间')}=%s "
                    f"WHERE user_id=%s AND {db_backend.quote_ident('收取时间')}=%s",
                    (harvested_at, user_id, expected_last_time),
                )
                if updated.rowcount != 1:
                    conn.rollback()
                    return MixelixirHarvestResult("state_changed", harvested_at, ())
                conn.execute(
                    "INSERT INTO mixelixir_harvest_operations (operation_id, payload, harvested_at, rewards_json) "
                    "VALUES (%s, %s, %s, %s)",
                    (operation_id, payload, harvested_at, rewards_json),
                )
                conn.commit()
                return MixelixirHarvestResult("applied", harvested_at, normalized_rewards)
            except Exception:
                conn.rollback()
                raise
            finally:
                if attached:
                    conn.execute("DETACH DATABASE player_data")

@dataclass(frozen=True)
class MixelixirHarvestLevelUpgradeResult:
    status: str
    cost: int = 0
    wallet_stone: int = 0
    level: int = 0
    experience: int = 0

    @property
    def succeeded(self) -> bool:
        return self.status in {"applied", "duplicate"}

class MixelixirHarvestLevelUpgradeService:
    """Atomically charge stones and upgrade the herb harvest level."""

    _LEVEL_FIELD = "收取等级"
    _EXPERIENCE_FIELD = "炼丹经验"

    def __init__(self, game_database: str | Path, player_database: str | Path, lock: RLock | None = None) -> None:
        self._game_database = Path(game_database)
        self._player_database = Path(player_database)
        self._lock = lock or RLock()

    def get_result(self, operation_id: str) -> MixelixirHarvestLevelUpgradeResult | None:
        operation_id = str(operation_id).strip()
        if not operation_id:
            return None
        with self._lock, closing(db_backend.connect(self._game_database)) as conn:
            conn.execute(
                "CREATE TABLE IF NOT EXISTS mixelixir_harvest_level_upgrade_operations ("
                "operation_id TEXT PRIMARY KEY,payload TEXT NOT NULL,cost INTEGER NOT NULL,"
                "wallet_stone INTEGER NOT NULL,level INTEGER NOT NULL,experience INTEGER NOT NULL,"
                "created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP)"
            )
            previous = conn.execute(
                "SELECT payload,cost,wallet_stone,level,experience FROM mixelixir_harvest_level_upgrade_operations WHERE operation_id=%s",
                (operation_id,),
            ).fetchone()
            if previous is None:
                return None
            return MixelixirHarvestLevelUpgradeResult(
                "duplicate", int(previous[1]), int(previous[2]), int(previous[3]), int(previous[4])
            )

    def upgrade(
        self,
        operation_id,
        user_id,
        expected_level,
        expected_experience,
        expected_stone,
        next_level,
        cost,
    ) -> MixelixirHarvestLevelUpgradeResult:
        operation_id, user_id = str(operation_id).strip(), str(user_id)
        expected_level, expected_experience, expected_stone, next_level, cost = map(
            int, (expected_level, expected_experience, expected_stone, next_level, cost)
        )
        if (
            not operation_id
            or min(expected_level, expected_experience, expected_stone) < 0
            or next_level != expected_level + 1
            or cost <= 0
        ):
            raise ValueError("valid operation, state snapshot, next level and cost are required")
        # Request identity only — expected level/exp/stone are concurrency checks.
        payload = json.dumps(
            [user_id, next_level, cost], ensure_ascii=True, separators=(",", ":"),
        )

        def result(status, *, stone=expected_stone, level=expected_level):
            return MixelixirHarvestLevelUpgradeResult(status, 0, stone, level, expected_experience)

        with self._lock, closing(db_backend.connect(self._game_database)) as conn:
            attached = False
            try:
                conn.execute("ATTACH DATABASE %s AS player_data", (str(self._player_database),))
                attached = True
                conn.execute("BEGIN IMMEDIATE")
                conn.execute(
                    "CREATE TABLE IF NOT EXISTS mixelixir_harvest_level_upgrade_operations ("
                    "operation_id TEXT PRIMARY KEY,payload TEXT NOT NULL,cost INTEGER NOT NULL,"
                    "wallet_stone INTEGER NOT NULL,level INTEGER NOT NULL,experience INTEGER NOT NULL,"
                    "created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP)"
                )
                previous = conn.execute(
                    "SELECT payload,cost,wallet_stone,level,experience "
                    "FROM mixelixir_harvest_level_upgrade_operations WHERE operation_id=%s",
                    (operation_id,),
                ).fetchone()
                if previous is not None:
                    conn.rollback()
                    if str(previous[0]) != payload:
                        return result("state_changed")
                    return MixelixirHarvestLevelUpgradeResult(
                        "duplicate", int(previous[1]), int(previous[2]), int(previous[3]), int(previous[4])
                    )

                user = conn.execute(
                    "SELECT COALESCE(stone,0) FROM user_xiuxian WHERE user_id=%s", (user_id,)
                ).fetchone()
                if user is None:
                    conn.rollback()
                    return result("user_missing", stone=0)

                table = conn.execute(
                    "SELECT 1 FROM player_data.sqlite_master WHERE type='table' AND name=%s",
                    ("mix_elixir_info",),
                ).fetchone()
                if table is None:
                    conn.rollback()
                    return result("state_changed", stone=int(user[0]))
                columns = {
                    str(column[1])
                    for column in conn.execute("PRAGMA player_data.table_info(mix_elixir_info)").fetchall()
                }
                if not {self._LEVEL_FIELD, self._EXPERIENCE_FIELD}.issubset(columns):
                    conn.rollback()
                    return result("state_changed", stone=int(user[0]))

                quoted_level = db_backend.quote_ident(self._LEVEL_FIELD)
                quoted_experience = db_backend.quote_ident(self._EXPERIENCE_FIELD)
                mix_state = conn.execute(
                    f"SELECT {quoted_level},{quoted_experience} FROM player_data.mix_elixir_info WHERE user_id=%s",
                    (user_id,),
                ).fetchone()
                current_stone = int(user[0])
                if (
                    mix_state is None
                    or current_stone != expected_stone
                    or (int(mix_state[0] or 0), int(mix_state[1] or 0))
                    != (expected_level, expected_experience)
                ):
                    conn.rollback()
                    return result("state_changed", stone=current_stone)
                if current_stone < cost:
                    conn.rollback()
                    return result("stone_insufficient", stone=current_stone)

                charged = conn.execute(
                    "UPDATE user_xiuxian SET stone=stone-%s WHERE user_id=%s AND stone=%s AND stone>=%s",
                    (cost, user_id, expected_stone, cost),
                )
                upgraded = conn.execute(
                    f"UPDATE player_data.mix_elixir_info SET {quoted_level}=%s "
                    f"WHERE user_id=%s AND CAST({quoted_level} AS INTEGER)=%s "
                    f"AND CAST({quoted_experience} AS INTEGER)=%s",
                    (str(next_level), user_id, expected_level, expected_experience),
                )
                if charged.rowcount != 1 or upgraded.rowcount != 1:
                    conn.rollback()
                    return result("state_changed", stone=current_stone)

                wallet_stone = expected_stone - cost
                conn.execute(
                    "INSERT INTO mixelixir_harvest_level_upgrade_operations "
                    "(operation_id,payload,cost,wallet_stone,level,experience) VALUES (%s,%s,%s,%s,%s,%s)",
                    (operation_id, payload, cost, wallet_stone, next_level, expected_experience),
                )
                conn.commit()
                return MixelixirHarvestLevelUpgradeResult(
                    "applied", cost, wallet_stone, next_level, expected_experience
                )
            except Exception:
                conn.rollback()
                raise
            finally:
                if attached:
                    conn.execute("DETACH DATABASE player_data")

@dataclass(frozen=True)
class MixelixirFireControlUpgradeResult:
    status: str
    cost: int = 0
    wallet_stone: int = 0
    level: int = 0
    experience: int = 0

    @property
    def succeeded(self) -> bool:
        return self.status in {"applied", "duplicate"}

class MixelixirFireControlUpgradeService:
    """Atomically charge stones and upgrade the alchemy fire-control level."""

    _LEVEL_FIELD = "丹药控火"
    _EXPERIENCE_FIELD = "炼丹经验"

    def __init__(self, game_database: str | Path, player_database: str | Path, lock: RLock | None = None) -> None:
        self._game_database = Path(game_database)
        self._player_database = Path(player_database)
        self._lock = lock or RLock()

    def get_result(self, operation_id: str) -> MixelixirFireControlUpgradeResult | None:
        operation_id = str(operation_id).strip()
        if not operation_id:
            return None
        with self._lock, closing(db_backend.connect(self._game_database)) as conn:
            conn.execute(
                "CREATE TABLE IF NOT EXISTS mixelixir_fire_control_upgrade_operations ("
                "operation_id TEXT PRIMARY KEY,payload TEXT NOT NULL,cost INTEGER NOT NULL,"
                "wallet_stone INTEGER NOT NULL,level INTEGER NOT NULL,experience INTEGER NOT NULL,"
                "created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP)"
            )
            previous = conn.execute(
                "SELECT payload,cost,wallet_stone,level,experience FROM mixelixir_fire_control_upgrade_operations WHERE operation_id=%s",
                (operation_id,),
            ).fetchone()
            if previous is None:
                return None
            return MixelixirFireControlUpgradeResult(
                "duplicate", int(previous[1]), int(previous[2]), int(previous[3]), int(previous[4])
            )

    def upgrade(
        self,
        operation_id,
        user_id,
        expected_level,
        expected_experience,
        expected_stone,
        next_level,
        cost,
    ) -> MixelixirFireControlUpgradeResult:
        operation_id, user_id = str(operation_id).strip(), str(user_id)
        expected_level, expected_experience, expected_stone, next_level, cost = map(
            int, (expected_level, expected_experience, expected_stone, next_level, cost)
        )
        if (
            not operation_id
            or min(expected_level, expected_experience, expected_stone) < 0
            or next_level != expected_level + 1
            or cost <= 0
        ):
            raise ValueError("valid operation, state snapshot, next level and cost are required")
        # Request identity only — expected level/exp/stone are concurrency checks.
        payload = json.dumps(
            [user_id, next_level, cost], ensure_ascii=True, separators=(",", ":"),
        )

        def result(status, *, stone=expected_stone, level=expected_level):
            return MixelixirFireControlUpgradeResult(status, 0, stone, level, expected_experience)

        with self._lock, closing(db_backend.connect(self._game_database)) as conn:
            attached = False
            try:
                conn.execute("ATTACH DATABASE %s AS player_data", (str(self._player_database),))
                attached = True
                conn.execute("BEGIN IMMEDIATE")
                conn.execute(
                    "CREATE TABLE IF NOT EXISTS mixelixir_fire_control_upgrade_operations ("
                    "operation_id TEXT PRIMARY KEY,payload TEXT NOT NULL,cost INTEGER NOT NULL,"
                    "wallet_stone INTEGER NOT NULL,level INTEGER NOT NULL,experience INTEGER NOT NULL,"
                    "created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP)"
                )
                previous = conn.execute(
                    "SELECT payload,cost,wallet_stone,level,experience "
                    "FROM mixelixir_fire_control_upgrade_operations WHERE operation_id=%s",
                    (operation_id,),
                ).fetchone()
                if previous is not None:
                    conn.rollback()
                    if str(previous[0]) != payload:
                        return result("state_changed")
                    return MixelixirFireControlUpgradeResult(
                        "duplicate", int(previous[1]), int(previous[2]), int(previous[3]), int(previous[4])
                    )

                user = conn.execute(
                    "SELECT COALESCE(stone,0) FROM user_xiuxian WHERE user_id=%s", (user_id,)
                ).fetchone()
                if user is None:
                    conn.rollback()
                    return result("user_missing", stone=0)

                table = conn.execute(
                    "SELECT 1 FROM player_data.sqlite_master WHERE type='table' AND name=%s",
                    ("mix_elixir_info",),
                ).fetchone()
                if table is None:
                    conn.rollback()
                    return result("state_changed", stone=int(user[0]))
                columns = {
                    str(column[1])
                    for column in conn.execute("PRAGMA player_data.table_info(mix_elixir_info)").fetchall()
                }
                if not {self._LEVEL_FIELD, self._EXPERIENCE_FIELD}.issubset(columns):
                    conn.rollback()
                    return result("state_changed", stone=int(user[0]))

                quoted_level = db_backend.quote_ident(self._LEVEL_FIELD)
                quoted_experience = db_backend.quote_ident(self._EXPERIENCE_FIELD)
                mix_state = conn.execute(
                    f"SELECT {quoted_level},{quoted_experience} FROM player_data.mix_elixir_info WHERE user_id=%s",
                    (user_id,),
                ).fetchone()
                current_stone = int(user[0])
                if (
                    mix_state is None
                    or current_stone != expected_stone
                    or (int(mix_state[0] or 0), int(mix_state[1] or 0))
                    != (expected_level, expected_experience)
                ):
                    conn.rollback()
                    return result("state_changed", stone=current_stone)
                if current_stone < cost:
                    conn.rollback()
                    return result("stone_insufficient", stone=current_stone)

                charged = conn.execute(
                    "UPDATE user_xiuxian SET stone=stone-%s WHERE user_id=%s AND stone=%s AND stone>=%s",
                    (cost, user_id, expected_stone, cost),
                )
                upgraded = conn.execute(
                    f"UPDATE player_data.mix_elixir_info SET {quoted_level}=%s "
                    f"WHERE user_id=%s AND CAST({quoted_level} AS INTEGER)=%s "
                    f"AND CAST({quoted_experience} AS INTEGER)=%s",
                    (str(next_level), user_id, expected_level, expected_experience),
                )
                if charged.rowcount != 1 or upgraded.rowcount != 1:
                    conn.rollback()
                    return result("state_changed", stone=current_stone)

                wallet_stone = expected_stone - cost
                conn.execute(
                    "INSERT INTO mixelixir_fire_control_upgrade_operations "
                    "(operation_id,payload,cost,wallet_stone,level,experience) VALUES (%s,%s,%s,%s,%s,%s)",
                    (operation_id, payload, cost, wallet_stone, next_level, expected_experience),
                )
                conn.commit()
                return MixelixirFireControlUpgradeResult(
                    "applied", cost, wallet_stone, next_level, expected_experience
                )
            except Exception:
                conn.rollback()
                raise
            finally:
                if attached:
                    conn.execute("DETACH DATABASE player_data")

@dataclass(frozen=True)
class MixelixirRefineCostResult:
    status: str
    task_id: str

    @property
    def succeeded(self) -> bool:
        return self.status in {"applied", "duplicate"}

class MixelixirRefineCostService:
    """Consume a saved recipe snapshot and create one claimable refining task."""

    def __init__(self, database: str | Path, lock: RLock | None = None) -> None:
        self._database = Path(database)
        self._lock = lock or RLock()

    def get_result(self, operation_id: str) -> MixelixirRefineCostResult | None:
        operation_id = str(operation_id).strip()
        if not operation_id:
            return None
        with self._lock, closing(db_backend.connect(self._database)) as conn:
            conn.execute(
                "CREATE TABLE IF NOT EXISTS mixelixir_refine_cost_operations ("
                "operation_id TEXT PRIMARY KEY,payload TEXT NOT NULL,task_id TEXT NOT NULL,"
                "created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP)"
            )
            previous = conn.execute(
                "SELECT payload,task_id FROM mixelixir_refine_cost_operations WHERE operation_id=%s", (operation_id,),
            ).fetchone()
            if previous is None:
                return None
            return MixelixirRefineCostResult("duplicate", str(previous[1]))

    def start(
        self,
        operation_id,
        user_id,
        recipe_set_id,
        recipe_key,
        expected_daily_count,
        reward_quantity,
        expected_mix_state,
        updated_mix_state,
    ) -> MixelixirRefineCostResult:
        operation_id, user_id = str(operation_id).strip(), str(user_id)
        recipe_set_id, recipe_key = str(recipe_set_id), str(recipe_key)
        expected_daily_count, reward_quantity = int(expected_daily_count), int(reward_quantity)
        expected_mix_state, updated_mix_state = dict(expected_mix_state), dict(updated_mix_state)
        if not operation_id or not recipe_set_id or not recipe_key or expected_daily_count < 0 or reward_quantity <= 0:
            raise ValueError("valid operation, saved recipe and reward snapshot are required")
        task_id = operation_id
        # Request identity only — daily/mix/reward snapshots are concurrency/outcome.
        payload = json.dumps(
            [user_id, recipe_set_id, recipe_key], ensure_ascii=True, separators=(",", ":"),
        )

        with self._lock, closing(db_backend.connect(self._database)) as conn:
            try:
                conn.execute("BEGIN IMMEDIATE")
                conn.execute(
                    "CREATE TABLE IF NOT EXISTS mixelixir_refine_tasks ("
                    "task_id TEXT PRIMARY KEY,user_id TEXT NOT NULL,recipe_set_id TEXT NOT NULL,recipe_key TEXT NOT NULL,"
                    "status TEXT NOT NULL,materials_json TEXT NOT NULL,reward_id INTEGER NOT NULL,reward_name TEXT NOT NULL,"
                    "reward_quantity INTEGER NOT NULL,expected_mix_state TEXT NOT NULL,updated_mix_state TEXT NOT NULL,"
                    "created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,claimed_at TIMESTAMP)"
                )
                conn.execute(
                    "CREATE TABLE IF NOT EXISTS mixelixir_refine_cost_operations ("
                    "operation_id TEXT PRIMARY KEY,payload TEXT NOT NULL,task_id TEXT NOT NULL,"
                    "created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP)"
                )
                previous = conn.execute(
                    "SELECT payload,task_id FROM mixelixir_refine_cost_operations WHERE operation_id=%s", (operation_id,)
                ).fetchone()
                if previous is not None:
                    conn.rollback()
                    if str(previous[0]) != payload:
                        return MixelixirRefineCostResult("state_changed", "")
                    return MixelixirRefineCostResult("duplicate", str(previous[1]))

                user = conn.execute(
                    "SELECT COALESCE(mixelixir_num,0) FROM user_xiuxian WHERE user_id=%s", (user_id,)
                ).fetchone()
                if user is None:
                    conn.rollback()
                    return MixelixirRefineCostResult("user_missing", "")
                if int(user[0]) != expected_daily_count:
                    conn.rollback()
                    return MixelixirRefineCostResult("state_changed", "")
                if expected_daily_count >= 100:
                    conn.rollback()
                    return MixelixirRefineCostResult("limit_reached", "")

                recipe_row = conn.execute(
                    "SELECT daily_count,materials_json,furnaces_json,recipes_json FROM mixelixir_recipe_sets "
                    "WHERE user_id=%s AND recipe_set_id=%s",
                    (user_id, recipe_set_id),
                ).fetchone()
                if recipe_row is None or int(recipe_row[0]) != expected_daily_count:
                    conn.rollback()
                    return MixelixirRefineCostResult("state_changed", "")
                recipes = json.loads(str(recipe_row[3]))
                recipe = next((item for item in recipes if str(item.get("recipe_key", "")) == recipe_key), None)
                if recipe is None:
                    conn.rollback()
                    return MixelixirRefineCostResult("recipe_missing", "")

                material_snapshot = {int(row[0]): int(row[2]) for row in json.loads(str(recipe_row[1]))}
                materials: dict[int, int] = {}
                for material in recipe["materials"]:
                    item_id, quantity = int(material["id"]), int(material["quantity"])
                    materials[item_id] = materials.get(item_id, 0) + quantity
                for item_id, quantity in materials.items():
                    row = conn.execute(
                        "SELECT COALESCE(goods_num,0),goods_name,goods_type FROM back WHERE user_id=%s AND goods_id=%s",
                        (user_id, item_id),
                    ).fetchone()
                    if row is None or int(row[0]) < quantity:
                        conn.rollback()
                        return MixelixirRefineCostResult("item_insufficient", "")
                    if int(row[0]) != material_snapshot.get(item_id) or str(row[2]) != "药材":
                        conn.rollback()
                        return MixelixirRefineCostResult("state_changed", "")

                furnace = dict(recipe["furnace"])
                furnace_snapshot = {int(row[0]): (str(row[1]), int(row[2])) for row in json.loads(str(recipe_row[2]))}
                furnace_row = conn.execute(
                    "SELECT goods_name,COALESCE(goods_num,0),goods_type FROM back WHERE user_id=%s AND goods_id=%s",
                    (user_id, int(furnace["id"])),
                ).fetchone()
                if furnace_row is None or furnace_snapshot.get(int(furnace["id"])) != (str(furnace_row[0]), int(furnace_row[1])) or str(furnace_row[2]) != "炼丹炉":
                    conn.rollback()
                    return MixelixirRefineCostResult("state_changed", "")

                for item_id, quantity in materials.items():
                    changed = conn.execute(
                        "UPDATE back SET goods_num=goods_num-%s WHERE user_id=%s AND goods_id=%s AND goods_num=%s",
                        (quantity, user_id, item_id, material_snapshot[item_id]),
                    )
                    if changed.rowcount != 1:
                        conn.rollback()
                        return MixelixirRefineCostResult("state_changed", "")
                changed = conn.execute(
                    "UPDATE user_xiuxian SET mixelixir_num=mixelixir_num+1 WHERE user_id=%s AND COALESCE(mixelixir_num,0)=%s",
                    (user_id, expected_daily_count),
                )
                if changed.rowcount != 1:
                    conn.rollback()
                    return MixelixirRefineCostResult("state_changed", "")

                conn.execute(
                    "INSERT INTO mixelixir_refine_tasks "
                    "(task_id,user_id,recipe_set_id,recipe_key,status,materials_json,reward_id,reward_name,"
                    "reward_quantity,expected_mix_state,updated_mix_state) VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)",
                    (
                        task_id,
                        user_id,
                        recipe_set_id,
                        recipe_key,
                        "ready",
                        json.dumps(sorted(materials.items())),
                        int(recipe["reward_id"]),
                        str(recipe["reward_name"]),
                        reward_quantity,
                        json.dumps(expected_mix_state, ensure_ascii=False, sort_keys=True),
                        json.dumps(updated_mix_state, ensure_ascii=False, sort_keys=True),
                    ),
                )
                conn.execute(
                    "INSERT INTO mixelixir_refine_cost_operations (operation_id,payload,task_id) VALUES (%s,%s,%s)",
                    (operation_id, payload, task_id),
                )
                conn.commit()
                return MixelixirRefineCostResult("applied", task_id)
            except Exception:
                conn.rollback()
                raise

@dataclass(frozen=True)
class MixelixirRefineRewardResult:
    status: str
    reward_id: int = 0
    reward_name: str = ""
    reward_quantity: int = 0

    @property
    def succeeded(self) -> bool:
        return self.status in {"applied", "duplicate"}

class MixelixirRefineRewardService:
    """Atomically complete a refining task, grant its item and save claim state."""

    _MIX_FIELDS = ("丹药控火", "炼丹记录", "炼丹经验")

    def __init__(self, game_database: str | Path, player_database: str | Path, lock: RLock | None = None) -> None:
        self._game_database = Path(game_database)
        self._player_database = Path(player_database)
        self._lock = lock or RLock()

    @staticmethod
    def _state_value(field: str, value) -> str:
        if field == "炼丹记录":
            return json.dumps(value, ensure_ascii=False, sort_keys=True)
        return str(value)

    def latest_ready_task(self, user_id) -> str | None:
        with self._lock, closing(db_backend.connect(self._game_database)) as conn:
            row = conn.execute(
                "SELECT task_id FROM mixelixir_refine_tasks WHERE user_id=%s AND status=%s "
                "ORDER BY created_at DESC,task_id DESC LIMIT 1",
                (str(user_id), "ready"),
            ).fetchone()
        return None if row is None else str(row[0])

    def get_result(self, operation_id: str) -> MixelixirRefineRewardResult | None:
        operation_id = str(operation_id).strip()
        if not operation_id:
            return None
        with self._lock, closing(db_backend.connect(self._game_database)) as conn:
            conn.execute(
                "CREATE TABLE IF NOT EXISTS mixelixir_refine_reward_operations ("
                "operation_id TEXT PRIMARY KEY,payload TEXT NOT NULL,task_id TEXT NOT NULL,reward_id INTEGER NOT NULL,"
                "reward_name TEXT NOT NULL,reward_quantity INTEGER NOT NULL,created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP)"
            )
            previous = conn.execute(
                "SELECT payload,reward_id,reward_name,reward_quantity FROM mixelixir_refine_reward_operations "
                "WHERE operation_id=%s",
                (operation_id,),
            ).fetchone()
            if previous is None:
                return None
            return MixelixirRefineRewardResult("duplicate", int(previous[1]), str(previous[2]), int(previous[3]))

    def claim(self, operation_id, user_id, task_id, max_goods_num) -> MixelixirRefineRewardResult:
        operation_id, user_id, task_id = str(operation_id).strip(), str(user_id), str(task_id)
        max_goods_num = int(max_goods_num)
        if not operation_id or not task_id or max_goods_num <= 0:
            raise ValueError("valid operation, task and capacity are required")
        # Request identity only — max_goods_num is capacity config.
        payload = json.dumps([user_id, task_id], ensure_ascii=True, separators=(",", ":"))

        with self._lock, closing(db_backend.connect(self._game_database)) as conn:
            attached = False
            try:
                conn.execute("ATTACH DATABASE %s AS player_data", (str(self._player_database),))
                attached = True
                conn.execute("BEGIN IMMEDIATE")
                conn.execute(
                    "CREATE TABLE IF NOT EXISTS mixelixir_refine_reward_operations ("
                    "operation_id TEXT PRIMARY KEY,payload TEXT NOT NULL,task_id TEXT NOT NULL,reward_id INTEGER NOT NULL,"
                    "reward_name TEXT NOT NULL,reward_quantity INTEGER NOT NULL,created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP)"
                )
                previous = conn.execute(
                    "SELECT payload,reward_id,reward_name,reward_quantity FROM mixelixir_refine_reward_operations "
                    "WHERE operation_id=%s",
                    (operation_id,),
                ).fetchone()
                if previous is not None:
                    conn.rollback()
                    if str(previous[0]) != payload:
                        return MixelixirRefineRewardResult("state_changed")
                    return MixelixirRefineRewardResult("duplicate", int(previous[1]), str(previous[2]), int(previous[3]))

                if conn.execute("SELECT 1 FROM user_xiuxian WHERE user_id=%s", (user_id,)).fetchone() is None:
                    conn.rollback()
                    return MixelixirRefineRewardResult("user_missing")
                task = conn.execute(
                    "SELECT status,reward_id,reward_name,reward_quantity,expected_mix_state,updated_mix_state "
                    "FROM mixelixir_refine_tasks WHERE task_id=%s AND user_id=%s",
                    (task_id, user_id),
                ).fetchone()
                if task is None:
                    conn.rollback()
                    return MixelixirRefineRewardResult("task_missing")
                if str(task[0]) != "ready":
                    conn.rollback()
                    return MixelixirRefineRewardResult("state_changed")
                reward_id, reward_name, reward_quantity = int(task[1]), str(task[2]), int(task[3])
                expected, updated = json.loads(str(task[4])), json.loads(str(task[5]))

                table = conn.execute(
                    "SELECT 1 FROM player_data.sqlite_master WHERE type='table' AND name=%s", ("mix_elixir_info",)
                ).fetchone()
                if table is None:
                    conn.rollback()
                    return MixelixirRefineRewardResult("state_changed")
                columns = {
                    str(column[1])
                    for column in conn.execute("PRAGMA player_data.table_info(mix_elixir_info)").fetchall()
                }
                if not set(self._MIX_FIELDS).issubset(columns):
                    conn.rollback()
                    return MixelixirRefineRewardResult("state_changed")
                quoted = ",".join(db_backend.quote_ident(field) for field in self._MIX_FIELDS)
                current = conn.execute(
                    f"SELECT {quoted} FROM player_data.mix_elixir_info WHERE user_id=%s", (user_id,)
                ).fetchone()
                if current is None or tuple(str(value) for value in current) != tuple(
                    self._state_value(field, expected[field]) for field in self._MIX_FIELDS
                ):
                    conn.rollback()
                    return MixelixirRefineRewardResult("state_changed")

                inventory = conn.execute(
                    "SELECT COALESCE(goods_num,0) FROM back WHERE user_id=%s AND goods_id=%s", (user_id, reward_id)
                ).fetchone()
                if (int(inventory[0]) if inventory else 0) + reward_quantity > max_goods_num:
                    conn.rollback()
                    return MixelixirRefineRewardResult("inventory_full")

                back_columns = set(conn.column_names("back"))
                insert_columns = "user_id,goods_id,goods_name,goods_type,goods_num"
                insert_values = "%s,%s,%s,%s,%s"
                if "bind_num" in back_columns:
                    insert_columns += ",bind_num"
                    insert_values += ",0"
                conn.execute(
                    f"INSERT INTO back ({insert_columns}) VALUES ({insert_values}) ON CONFLICT(user_id,goods_id) "
                    "DO UPDATE SET goods_name=EXCLUDED.goods_name,goods_type=EXCLUDED.goods_type,"
                    "goods_num=back.goods_num+EXCLUDED.goods_num",
                    (user_id, reward_id, reward_name, "丹药", reward_quantity),
                )
                assignments = ",".join(f"{db_backend.quote_ident(field)}=%s" for field in self._MIX_FIELDS)
                changed = conn.execute(
                    f"UPDATE player_data.mix_elixir_info SET {assignments} WHERE user_id=%s",
                    tuple(self._state_value(field, updated[field]) for field in self._MIX_FIELDS) + (user_id,),
                )
                if changed.rowcount != 1:
                    conn.rollback()
                    return MixelixirRefineRewardResult("state_changed")

                conn.execute("CREATE TABLE IF NOT EXISTS player_data.statistics (user_id TEXT PRIMARY KEY)")
                statistics_columns = {
                    str(column[1])
                    for column in conn.execute("PRAGMA player_data.table_info(statistics)").fetchall()
                }
                if "炼丹次数" not in statistics_columns:
                    conn.execute(
                        f"ALTER TABLE player_data.statistics ADD COLUMN {db_backend.quote_ident('炼丹次数')} INTEGER DEFAULT NULL"
                    )
                conn.execute(
                    f"INSERT INTO player_data.statistics (user_id,{db_backend.quote_ident('炼丹次数')}) VALUES (%s,1) "
                    f"ON CONFLICT(user_id) DO UPDATE SET {db_backend.quote_ident('炼丹次数')}="
                    f"COALESCE(player_data.statistics.{db_backend.quote_ident('炼丹次数')},0)+1",
                    (user_id,),
                )
                changed = conn.execute(
                    "UPDATE mixelixir_refine_tasks SET status=%s,claimed_at=CURRENT_TIMESTAMP "
                    "WHERE task_id=%s AND user_id=%s AND status=%s",
                    ("claimed", task_id, user_id, "ready"),
                )
                if changed.rowcount != 1:
                    conn.rollback()
                    return MixelixirRefineRewardResult("state_changed")
                conn.execute(
                    "INSERT INTO mixelixir_refine_reward_operations "
                    "(operation_id,payload,task_id,reward_id,reward_name,reward_quantity) VALUES (%s,%s,%s,%s,%s,%s)",
                    (operation_id, payload, task_id, reward_id, reward_name, reward_quantity),
                )
                conn.commit()
                return MixelixirRefineRewardResult("applied", reward_id, reward_name, reward_quantity)
            except Exception:
                conn.rollback()
                raise
            finally:
                if attached:
                    conn.execute("DETACH DATABASE player_data")

@dataclass(frozen=True)
class MixelixirSettlementResult:
    status: str
    reward_quantity: int

    @property
    def succeeded(self) -> bool:
        return self.status in {"applied", "duplicate"}

class MixelixirSettlementService:
    """Consume recipe materials and grant the elixir in one transaction."""

    def __init__(self, database: str | Path, lock: RLock | None = None) -> None:
        self._database = Path(database)
        self._lock = lock or RLock()

    def get_result(self, operation_id: str) -> MixelixirSettlementResult | None:
        operation_id = str(operation_id).strip()
        if not operation_id:
            return None
        with self._lock, closing(db_backend.connect(self._database)) as conn:
            conn.execute(
                "CREATE TABLE IF NOT EXISTS mixelixir_settlement_operations ("
                "operation_id TEXT PRIMARY KEY, payload TEXT NOT NULL, reward_quantity INTEGER NOT NULL, "
                "created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP)"
            )
            previous = conn.execute(
                "SELECT payload, reward_quantity FROM mixelixir_settlement_operations WHERE operation_id=%s",
                (operation_id,),
            ).fetchone()
            if previous is None:
                return None
            return MixelixirSettlementResult("duplicate", int(previous[1]))

    def settle(
        self,
        operation_id,
        user_id,
        materials,
        reward_id,
        reward_name,
        reward_quantity,
        *,
        max_goods_num,
    ) -> MixelixirSettlementResult:
        operation_id = str(operation_id).strip()
        user_id = str(user_id)
        reward_id = int(reward_id)
        reward_quantity = int(reward_quantity)
        max_goods_num = int(max_goods_num)
        normalized_materials: dict[int, int] = {}
        for item_id, quantity in materials.items():
            item_id = int(item_id)
            quantity = int(quantity)
            if quantity > 0:
                normalized_materials[item_id] = normalized_materials.get(item_id, 0) + quantity
        if not operation_id or not normalized_materials or reward_quantity <= 0 or max_goods_num <= 0:
            raise ValueError("operation, materials, reward quantity and capacity are required")

        # Request identity only — reward_name/max_goods_num are display/config.
        payload = json.dumps(
            [user_id, sorted(normalized_materials.items()), reward_id, reward_quantity],
            ensure_ascii=True, separators=(",", ":"),
        )
        with self._lock, closing(db_backend.connect(self._database)) as conn:
            try:
                conn.execute("BEGIN IMMEDIATE")
                conn.execute(
                    "CREATE TABLE IF NOT EXISTS mixelixir_settlement_operations ("
                    "operation_id TEXT PRIMARY KEY, payload TEXT NOT NULL, reward_quantity INTEGER NOT NULL, "
                    "created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP)"
                )
                previous = conn.execute(
                    "SELECT payload, reward_quantity FROM mixelixir_settlement_operations WHERE operation_id=%s",
                    (operation_id,),
                ).fetchone()
                if previous is not None:
                    conn.rollback()
                    if str(previous[0]) != payload:
                        return MixelixirSettlementResult("state_changed", 0)
                    return MixelixirSettlementResult("duplicate", int(previous[1]))

                user = conn.execute(
                    "SELECT 1 FROM user_xiuxian WHERE user_id=%s", (user_id,)
                ).fetchone()
                if user is None:
                    conn.rollback()
                    return MixelixirSettlementResult("user_missing", 0)

                for item_id, quantity in normalized_materials.items():
                    row = conn.execute(
                        "SELECT COALESCE(goods_num, 0) FROM back WHERE user_id=%s AND goods_id=%s",
                        (user_id, item_id),
                    ).fetchone()
                    if row is None or int(row[0]) < quantity:
                        conn.rollback()
                        return MixelixirSettlementResult("item_insufficient", 0)

                for item_id, quantity in normalized_materials.items():
                    consumed = conn.execute(
                        "UPDATE back SET goods_num=goods_num-%s WHERE user_id=%s AND goods_id=%s AND goods_num>=%s",
                        (quantity, user_id, item_id, quantity),
                    )
                    if consumed.rowcount != 1:
                        conn.rollback()
                        return MixelixirSettlementResult("state_changed", 0)

                columns = set(conn.column_names("back"))
                insert_columns = "user_id, goods_id, goods_name, goods_type, goods_num"
                insert_values = "%s, %s, %s, %s, %s"
                if "bind_num" in columns:
                    insert_columns += ", bind_num"
                    insert_values += ", 0"
                conn.execute(
                    f"INSERT INTO back ({insert_columns}) VALUES ({insert_values}) "
                    "ON CONFLICT (user_id, goods_id) DO UPDATE SET "
                    "goods_name=EXCLUDED.goods_name, goods_type=EXCLUDED.goods_type, "
                    "goods_num=MIN(COALESCE(back.goods_num, 0)+EXCLUDED.goods_num, %s)",
                    (user_id, reward_id, str(reward_name), "丹药", reward_quantity, max_goods_num),
                )
                updated = conn.execute(
                    "UPDATE user_xiuxian SET mixelixir_num=COALESCE(mixelixir_num, 0)+1 WHERE user_id=%s",
                    (user_id,),
                )
                if updated.rowcount != 1:
                    conn.rollback()
                    return MixelixirSettlementResult("state_changed", 0)
                conn.execute(
                    "INSERT INTO mixelixir_settlement_operations (operation_id, payload, reward_quantity) VALUES (%s, %s, %s)",
                    (operation_id, payload, reward_quantity),
                )
                conn.commit()
                return MixelixirSettlementResult("applied", reward_quantity)
            except Exception:
                conn.rollback()
                raise

__all__ = [
    "MixelixirRecipeSaveResult",
    "MixelixirRecipeService",
    "HarvestReward",
    "MixelixirHarvestResult",
    "MixelixirHarvestService",
    "MixelixirHarvestLevelUpgradeResult",
    "MixelixirHarvestLevelUpgradeService",
    "MixelixirFireControlUpgradeResult",
    "MixelixirFireControlUpgradeService",
    "MixelixirRefineCostResult",
    "MixelixirRefineCostService",
    "MixelixirRefineRewardResult",
    "MixelixirRefineRewardService",
    "MixelixirSettlementResult",
    "MixelixirSettlementService",
]
