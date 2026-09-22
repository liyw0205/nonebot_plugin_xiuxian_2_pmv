from __future__ import annotations

import json
from pathlib import Path

from ...infrastructure.database import DatabaseUnitOfWork


class MixelixirRefineCostResult:
    def __init__(self, status: str, task_id: str = "") -> None:
        self.status = status
        self.task_id = task_id


class MixelixirRefineCostSqlRepository:
    def __init__(self, database: str | Path) -> None:
        self.database = str(database)

    def start(self, operation_id: str, user_id: str, recipe_set_id: str, daily_count: int, expected_snapshot: dict, updated_mix_state: dict, max_goods_num: int) -> MixelixirRefineCostResult:
        operation_id, user_id, recipe_set_id = str(operation_id).strip(), str(user_id), str(recipe_set_id)
        payload = json.dumps([user_id, recipe_set_id, daily_count], ensure_ascii=False, separators=(",", ":"))
        task_id = f"{operation_id}:task"
        with DatabaseUnitOfWork(self.database, immediate=True) as uow:
            uow.execute("CREATE TABLE IF NOT EXISTS mixelixir_refine_cost_operations(operation_id TEXT PRIMARY KEY,payload TEXT NOT NULL,task_id TEXT NOT NULL,created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP)")
            previous = uow.query_one("SELECT payload,task_id FROM mixelixir_refine_cost_operations WHERE operation_id=?", (operation_id,))
            if previous is not None:
                if str(previous["payload"]) != payload:
                    return MixelixirRefineCostResult("state_changed")
                return MixelixirRefineCostResult("duplicate", str(previous["task_id"]))
            recipe = uow.query_one("SELECT materials_json,furnaces_json,recipes_json FROM mixelixir_recipe_sets WHERE user_id=? AND recipe_set_id=? AND daily_count=?", (user_id, recipe_set_id, int(daily_count)))
            if recipe is None:
                return MixelixirRefineCostResult("state_changed")
            materials = json.loads(recipe["materials_json"] or "[]")
            for item_id, _, quantity in materials:
                row = uow.query_one("SELECT goods_num FROM back WHERE user_id=? AND goods_id=?", (user_id, int(item_id)))
                if row is None or int(row["goods_num"]) < int(quantity):
                    return MixelixirRefineCostResult("item_insufficient")
            for item_id, _, quantity in materials:
                uow.execute("UPDATE back SET goods_num=goods_num-? WHERE user_id=? AND goods_id=?", (int(quantity), user_id, int(item_id)))
            uow.execute("CREATE TABLE IF NOT EXISTS mixelixir_refine_tasks(task_id TEXT PRIMARY KEY,user_id TEXT NOT NULL,recipe_set_id TEXT NOT NULL,status TEXT NOT NULL,materials_json TEXT NOT NULL,reward_quantity INTEGER NOT NULL,expected_mix_state TEXT NOT NULL,updated_mix_state TEXT NOT NULL,created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,claimed_at TIMESTAMP)")
            uow.execute("INSERT INTO mixelixir_refine_tasks(task_id,user_id,recipe_set_id,status,materials_json,reward_quantity,expected_mix_state,updated_mix_state) VALUES(?,?,?,?,?,?,?,?)", (task_id, user_id, recipe_set_id, "ready", json.dumps(materials), 1, json.dumps(expected_snapshot, ensure_ascii=False), json.dumps(updated_mix_state, ensure_ascii=False)))
            uow.execute("INSERT INTO mixelixir_refine_cost_operations(operation_id,payload,task_id) VALUES(?,?,?)", (operation_id, payload, task_id))
            return MixelixirRefineCostResult("applied", task_id)


__all__ = ["MixelixirRefineCostSqlRepository", "MixelixirRefineCostResult"]
