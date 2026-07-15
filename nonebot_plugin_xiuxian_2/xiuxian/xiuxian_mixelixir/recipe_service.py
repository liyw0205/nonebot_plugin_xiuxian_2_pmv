from __future__ import annotations

import json
from contextlib import closing
from dataclasses import dataclass
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


__all__ = ["MixelixirRecipeSaveResult", "MixelixirRecipeService"]
