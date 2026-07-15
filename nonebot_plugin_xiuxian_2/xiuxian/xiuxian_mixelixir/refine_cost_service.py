from __future__ import annotations

import json
from contextlib import closing
from dataclasses import dataclass
from pathlib import Path
from threading import RLock

from ..xiuxian_utils import db_backend


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


__all__ = ["MixelixirRefineCostResult", "MixelixirRefineCostService"]
