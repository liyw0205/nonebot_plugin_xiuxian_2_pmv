from __future__ import annotations

import json
from collections.abc import Mapping
from pathlib import Path

from ...infrastructure.database import DatabaseUnitOfWork


class MixelixirRefineCostResult:
    def __init__(self, status: str, task_id: str = "", *, reason: str = "", subject: str = "") -> None:
        self.status = status
        self.task_id = task_id
        self.reason = reason
        self.subject = subject


class _KnownWriteRejection(Exception):
    def __init__(self, reason: str, subject: str) -> None:
        super().__init__(reason)
        self.reason = reason
        self.subject = subject


class MixelixirRefineCostSqlRepository:
    def __init__(self, database: str | Path) -> None:
        self.database = str(database)

    def start(
        self,
        operation_id: str,
        user_id: str,
        recipe_set_id: str,
        daily_count: int,
        expected_snapshot: dict,
        updated_mix_state: dict,
        max_goods_num: int,
        *,
        recipe_key: str,
        materials: Mapping[int, int],
        furnace_id: int,
        reward_id: int,
        reward_name: str,
        reward_quantity: int,
    ) -> MixelixirRefineCostResult:
        operation_id = str(operation_id).strip()
        user_id = str(user_id)
        recipe_set_id = str(recipe_set_id or "custom")
        recipe_key = str(recipe_key)
        daily_count, max_goods_num = int(daily_count), int(max_goods_num)
        furnace_id, reward_id = int(furnace_id), int(reward_id)
        reward_quantity = int(reward_quantity)
        normalized_materials = {
            int(item_id): int(quantity)
            for item_id, quantity in dict(materials or {}).items()
            if int(quantity) > 0
        }
        if (
            not operation_id
            or not recipe_key
            or daily_count < 0
            or max_goods_num <= 0
            or furnace_id <= 0
            or reward_id <= 0
            or reward_quantity <= 0
            or not str(reward_name)
            or not normalized_materials
        ):
            raise ValueError("valid operation, recipe, materials and reward snapshot are required")
        if any(item_id <= 0 or quantity <= 0 for item_id, quantity in normalized_materials.items()):
            raise ValueError("material ids and quantities must be positive")

        payload = json.dumps(
            [user_id, recipe_set_id, recipe_key], ensure_ascii=True, separators=(",", ":")
        )
        task_id = f"{operation_id}:task"
        with DatabaseUnitOfWork(self.database, immediate=True) as uow:
            required_tables = {"user_xiuxian", "back", "mixelixir_refine_tasks", "mixelixir_refine_cost_operations"}
            tables = {
                str(row["name"])
                for row in uow.query_all("SELECT name FROM sqlite_master WHERE type='table'")
            }
            if not required_tables.issubset(tables):
                return MixelixirRefineCostResult("schema_missing")

            previous = uow.query_one(
                "SELECT payload,task_id FROM mixelixir_refine_cost_operations WHERE operation_id=?",
                (operation_id,),
            )
            if previous is not None:
                if str(previous["payload"]) != payload:
                    return MixelixirRefineCostResult("operation_conflict")
                return MixelixirRefineCostResult("duplicate", str(previous["task_id"]))

            user = uow.query_one(
                "SELECT COALESCE(mixelixir_num,0) AS daily_count FROM user_xiuxian WHERE user_id=?",
                (user_id,),
            )
            if user is None:
                return MixelixirRefineCostResult("user_missing")
            if int(user["daily_count"]) != daily_count:
                return MixelixirRefineCostResult("state_changed")
            if daily_count >= 100:
                return MixelixirRefineCostResult("limit_reached")

            for item_id, quantity in normalized_materials.items():
                row = uow.query_one(
                    "SELECT COALESCE(goods_num,0) AS goods_num,goods_type FROM back WHERE user_id=? AND goods_id=?",
                    (user_id, item_id),
                )
                if row is None or int(row["goods_num"]) < quantity or str(row["goods_type"]) != "药材":
                    return MixelixirRefineCostResult("item_insufficient")

            furnace = uow.query_one(
                "SELECT COALESCE(goods_num,0) AS goods_num,goods_type FROM back WHERE user_id=? AND goods_id=?",
                (user_id, furnace_id),
            )
            if furnace is None or int(furnace["goods_num"]) < 1 or str(furnace["goods_type"]) != "炼丹炉":
                return MixelixirRefineCostResult("state_changed")

            before_materials = {
                item_id: int(uow.query_one(
                    "SELECT COALESCE(goods_num,0) AS goods_num FROM back WHERE user_id=? AND goods_id=?",
                    (user_id, item_id),
                )["goods_num"])
                for item_id in normalized_materials
            }
            task_materials = json.dumps(sorted(normalized_materials.items()), separators=(",", ":"))
            expected_json = json.dumps(expected_snapshot, ensure_ascii=False, sort_keys=True, separators=(",", ":"))
            updated_json = json.dumps(updated_mix_state, ensure_ascii=False, sort_keys=True, separators=(",", ":"))
            try:
                with uow.savepoint("mixelixir_refine_cost"):
                    for item_id, quantity in normalized_materials.items():
                        changed = uow.execute(
                            "UPDATE back SET goods_num=goods_num-? WHERE user_id=? AND goods_id=? AND COALESCE(goods_num,0)>=?",
                            (quantity, user_id, item_id, quantity),
                        )
                        if changed.rowcount != 1:
                            raise _KnownWriteRejection("mixelixir_refine_cost_material_write_rejected", f"item:{item_id}")
                        current = uow.query_one(
                            "SELECT COALESCE(goods_num,0) AS goods_num FROM back WHERE user_id=? AND goods_id=?",
                            (user_id, item_id),
                        )
                        if current is None or int(current["goods_num"]) != before_materials[item_id] - quantity:
                            raise _KnownWriteRejection("mixelixir_refine_cost_material_projection_mismatch", f"item:{item_id}")

                    changed = uow.execute(
                        "UPDATE user_xiuxian SET mixelixir_num=COALESCE(mixelixir_num,0)+1 "
                        "WHERE user_id=? AND COALESCE(mixelixir_num,0)=?",
                        (user_id, daily_count),
                    )
                    if changed.rowcount != 1:
                        raise _KnownWriteRejection("mixelixir_refine_cost_daily_write_rejected", "daily_count")
                    current_daily = uow.query_one(
                        "SELECT COALESCE(mixelixir_num,0) AS mixelixir_num FROM user_xiuxian WHERE user_id=?",
                        (user_id,),
                    )
                    if current_daily is None or int(current_daily["mixelixir_num"]) != daily_count + 1:
                        raise _KnownWriteRejection("mixelixir_refine_cost_daily_projection_mismatch", "daily_count")

                    task_insert = uow.execute(
                        "INSERT INTO mixelixir_refine_tasks "
                        "(task_id,user_id,recipe_set_id,recipe_key,status,materials_json,reward_id,reward_name,"
                        "reward_quantity,expected_mix_state,updated_mix_state) VALUES(?,?,?,?,?,?,?,?,?,?,?)",
                        (
                            task_id, user_id, recipe_set_id, recipe_key, "ready", task_materials,
                            reward_id, str(reward_name), reward_quantity, expected_json, updated_json,
                        ),
                    )
                    if task_insert.rowcount != 1:
                        raise _KnownWriteRejection("mixelixir_refine_cost_task_write_rejected", "task")
                    saved_task = uow.query_one(
                        "SELECT user_id,recipe_set_id,recipe_key,status,materials_json,reward_id,reward_name,"
                        "reward_quantity,expected_mix_state,updated_mix_state FROM mixelixir_refine_tasks WHERE task_id=?",
                        (task_id,),
                    )
                    expected_task = (
                        user_id, recipe_set_id, recipe_key, "ready", task_materials,
                        reward_id, str(reward_name), reward_quantity, expected_json, updated_json,
                    )
                    if saved_task is None or tuple(saved_task.values()) != expected_task:
                        raise _KnownWriteRejection("mixelixir_refine_cost_task_projection_mismatch", "task")

                    receipt_insert = uow.execute(
                        "INSERT INTO mixelixir_refine_cost_operations(operation_id,payload,task_id) VALUES(?,?,?)",
                        (operation_id, payload, task_id),
                    )
                    if receipt_insert.rowcount != 1:
                        raise _KnownWriteRejection("mixelixir_refine_cost_receipt_write_rejected", "receipt")
                    saved_receipt = uow.query_one(
                        "SELECT payload,task_id FROM mixelixir_refine_cost_operations WHERE operation_id=?",
                        (operation_id,),
                    )
                    if saved_receipt is None or tuple(saved_receipt.values()) != (payload, task_id):
                        raise _KnownWriteRejection("mixelixir_refine_cost_receipt_projection_mismatch", "receipt")

                    for item_id, quantity in normalized_materials.items():
                        final_material = uow.query_one(
                            "SELECT COALESCE(goods_num,0) AS goods_num FROM back WHERE user_id=? AND goods_id=?",
                            (user_id, item_id),
                        )
                        if final_material is None or int(final_material["goods_num"]) != before_materials[item_id] - quantity:
                            raise _KnownWriteRejection(
                                "mixelixir_refine_cost_material_projection_mismatch", f"item:{item_id}"
                            )
                    final_daily = uow.query_one(
                        "SELECT COALESCE(mixelixir_num,0) AS mixelixir_num FROM user_xiuxian WHERE user_id=?",
                        (user_id,),
                    )
                    if final_daily is None or int(final_daily["mixelixir_num"]) != daily_count + 1:
                        raise _KnownWriteRejection(
                            "mixelixir_refine_cost_daily_projection_mismatch", "daily_count"
                        )
                    final_task = uow.query_one(
                        "SELECT user_id,recipe_set_id,recipe_key,status,materials_json,reward_id,reward_name,"
                        "reward_quantity,expected_mix_state,updated_mix_state "
                        "FROM mixelixir_refine_tasks WHERE task_id=?",
                        (task_id,),
                    )
                    if final_task is None or tuple(final_task.values()) != expected_task:
                        raise _KnownWriteRejection(
                            "mixelixir_refine_cost_task_projection_mismatch", "task"
                        )
            except _KnownWriteRejection as rejected:
                return MixelixirRefineCostResult(
                    "write_rejected", reason=rejected.reason, subject=rejected.subject
                )
            return MixelixirRefineCostResult("applied", task_id)


__all__ = ["MixelixirRefineCostSqlRepository", "MixelixirRefineCostResult"]
