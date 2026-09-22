from __future__ import annotations

import json
from pathlib import Path

from ...infrastructure.database import DatabaseUnitOfWork


class MixelixirRefineRewardResult:
    def __init__(self, status: str, reward_id: int = 0, reward_name: str = "", reward_quantity: int = 0) -> None:
        self.status = status
        self.reward_id = reward_id
        self.reward_name = reward_name
        self.reward_quantity = reward_quantity


class MixelixirRefineRewardSqlRepository:
    def __init__(self, game_database: str | Path, player_database: str | Path) -> None:
        self.game_database = str(game_database)
        self.player_database = str(player_database)

    def claim(self, operation_id: str, user_id: str, task_id: str, max_goods_num: int) -> MixelixirRefineRewardResult:
        operation_id, user_id, task_id = str(operation_id).strip(), str(user_id), str(task_id)
        with DatabaseUnitOfWork(self.game_database, immediate=True) as uow:
            uow.execute("ATTACH DATABASE ? AS player_data", (self.player_database,))
            try:
                uow.execute("CREATE TABLE IF NOT EXISTS mixelixir_refine_reward_operations(operation_id TEXT PRIMARY KEY,payload TEXT NOT NULL,task_id TEXT NOT NULL,reward_id INTEGER NOT NULL,reward_name TEXT NOT NULL,reward_quantity INTEGER NOT NULL,created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP)")
                payload = json.dumps([user_id, task_id], separators=(",", ":"))
                previous = uow.query_one("SELECT payload,reward_id,reward_name,reward_quantity FROM mixelixir_refine_reward_operations WHERE operation_id=?", (operation_id,))
                if previous is not None:
                    if str(previous["payload"]) != payload:
                        return MixelixirRefineRewardResult("state_changed")
                    return MixelixirRefineRewardResult("duplicate", int(previous["reward_id"]), str(previous["reward_name"]), int(previous["reward_quantity"]))
                if uow.query_one("SELECT 1 FROM user_xiuxian WHERE user_id=?", (user_id,)) is None:
                    return MixelixirRefineRewardResult("user_missing")
                if uow.query_one("SELECT 1 FROM sqlite_master WHERE type='table' AND name='mixelixir_refine_tasks'") is None:
                    return MixelixirRefineRewardResult("task_missing")
                task = uow.query_one("SELECT status,reward_id,reward_name,reward_quantity FROM mixelixir_refine_tasks WHERE task_id=? AND user_id=?", (task_id, user_id))
                if task is None:
                    return MixelixirRefineRewardResult("task_missing")
                if str(task["status"]) != "ready":
                    return MixelixirRefineRewardResult("state_changed")
                reward_id, reward_name, quantity = int(task["reward_id"]), str(task["reward_name"]), int(task["reward_quantity"])
                inventory = uow.query_one("SELECT COALESCE(goods_num,0) AS goods_num FROM back WHERE user_id=? AND goods_id=?", (user_id, reward_id))
                if (int(inventory["goods_num"]) if inventory else 0) + quantity > int(max_goods_num):
                    return MixelixirRefineRewardResult("inventory_full")
                if inventory is None:
                    uow.execute("INSERT INTO back(user_id,goods_id,goods_name,goods_type,goods_num,bind_num) VALUES(?,?,?,?,?,0)", (user_id, reward_id, reward_name, "丹药", quantity))
                else:
                    uow.execute("UPDATE back SET goods_num=goods_num+? WHERE user_id=? AND goods_id=?", (quantity, user_id, reward_id))
                changed = uow.execute("UPDATE mixelixir_refine_tasks SET status='claimed',claimed_at=CURRENT_TIMESTAMP WHERE task_id=? AND user_id=? AND status='ready'", (task_id, user_id))
                if changed.rowcount != 1:
                    return MixelixirRefineRewardResult("state_changed")
                uow.execute("INSERT INTO mixelixir_refine_reward_operations(operation_id,payload,task_id,reward_id,reward_name,reward_quantity) VALUES(?,?,?,?,?,?)", (operation_id, payload, task_id, reward_id, reward_name, quantity))
                return MixelixirRefineRewardResult("applied", reward_id, reward_name, quantity)
            finally:
                pass


__all__ = ["MixelixirRefineRewardSqlRepository", "MixelixirRefineRewardResult"]
