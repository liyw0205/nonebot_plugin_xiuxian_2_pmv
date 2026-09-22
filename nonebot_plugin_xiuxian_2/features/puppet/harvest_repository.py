from __future__ import annotations

import json
from datetime import datetime
from pathlib import Path
from typing import Callable, Iterable

from ...infrastructure.database import DatabaseUnitOfWork


class PuppetHarvestSqlRepository:
    def __init__(self, game_database: str | Path, player_database: str | Path, *, max_goods_num: int) -> None:
        self.game_database = str(game_database)
        self.player_database = str(player_database)
        self.max_goods_num = int(max_goods_num)

    def harvest(self, user_id: str, *, operation_id: str, now: datetime, time_cost_hours: int, speed_base: float, harvest_costs: dict[int, int], harvest_bonus: int, reward_factory: Callable[[str, int], Iterable[tuple[int, str, str, int]]]):
        user_id, operation_id = str(user_id), str(operation_id).strip()
        with DatabaseUnitOfWork(self.game_database, immediate=True) as uow:
            uow.execute("ATTACH DATABASE ? AS player_data", (self.player_database,))
            try:
                uow.execute("CREATE TABLE IF NOT EXISTS puppet_harvest_operations(operation_id TEXT PRIMARY KEY,user_id TEXT NOT NULL,payload TEXT NOT NULL,rewards_json TEXT NOT NULL,stone_cost INTEGER NOT NULL,created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP)")
                previous = uow.query_one("SELECT rewards_json,stone_cost FROM puppet_harvest_operations WHERE operation_id=?", (operation_id,))
                if previous is not None:
                    rewards = json.loads(str(previous["rewards_json"]))
                    return type("PuppetHarvestResult", (), {"status":"duplicate","harvested":True,"rewards":tuple(type("Reward", (), reward)() for reward in rewards),"stone_cost":int(previous["stone_cost"])})()
                state = uow.query_one("SELECT \"收取时间\" AS last_time,\"灵田傀儡\" AS enabled,\"傀儡等级\" AS level FROM player_data.mix_elixir_info WHERE user_id=?", (user_id,))
                if state is None or int(state["enabled"] or 0) != 1:
                    return type("PuppetHarvestResult", (), {"status":"puppet_missing","harvested":False,"rewards":(),"stone_cost":0})()
                last = datetime.strptime(str(state["last_time"]), "%Y-%m-%d %H:%M:%S")
                elapsed = (now - last).total_seconds() / 3600
                remaining = elapsed * (1 + float(speed_base))
                if remaining < time_cost_hours:
                    return type("PuppetHarvestResult", (), {"status":"not_mature","harvested":False,"rewards":(),"stone_cost":0})()
                level = int(state["level"] or 1)
                stone_cost = int(harvest_costs.get(level, 0))
                wallet = uow.query_one("SELECT COALESCE(stone,0) AS stone FROM user_xiuxian WHERE user_id=?", (user_id,))
                if wallet is None or int(wallet["stone"]) < stone_cost:
                    return type("PuppetHarvestResult", (), {"status":"stone_insufficient","harvested":False,"rewards":(),"stone_cost":stone_cost})()
                rewards = []
                for item_id, name, item_type, quantity in reward_factory(str(level), int(remaining // time_cost_hours)):
                    rewards.append({"goods_id": int(item_id), "goods_name": str(name), "goods_type": str(item_type), "quantity": int(quantity) + int(harvest_bonus)})
                for reward in rewards:
                    existing = uow.query_one("SELECT COALESCE(goods_num,0) AS goods_num FROM back WHERE user_id=? AND goods_id=?", (user_id, reward["goods_id"]))
                    if existing is not None and int(existing["goods_num"]) + reward["quantity"] > self.max_goods_num:
                        return type("PuppetHarvestResult", (), {"status":"inventory_full","harvested":False,"rewards":(),"stone_cost":stone_cost})()
                uow.execute("UPDATE user_xiuxian SET stone=stone-? WHERE user_id=? AND stone>=?", (stone_cost, user_id, stone_cost))
                for reward in rewards:
                    existing = uow.query_one("SELECT goods_num FROM back WHERE user_id=? AND goods_id=?", (user_id, reward["goods_id"]))
                    if existing is None:
                        uow.execute("INSERT INTO back(user_id,goods_id,goods_name,goods_type,goods_num,bind_num) VALUES(?,?,?,?,?,0)", (user_id, reward["goods_id"], reward["goods_name"], reward["goods_type"], reward["quantity"]))
                    else:
                        uow.execute("UPDATE back SET goods_num=goods_num+? WHERE user_id=? AND goods_id=?", (reward["quantity"], user_id, reward["goods_id"]))
                harvested_at = now.strftime("%Y-%m-%d %H:%M:%S")
                uow.execute("UPDATE player_data.mix_elixir_info SET \"收取时间\"=? WHERE user_id=?", (harvested_at, user_id))
                uow.execute("INSERT INTO puppet_harvest_operations(operation_id,user_id,payload,rewards_json,stone_cost) VALUES(?,?,?,?,?)", (operation_id, user_id, json.dumps([user_id]), json.dumps(rewards), stone_cost))
                return type("PuppetHarvestResult", (), {"status":"harvested","harvested":True,"rewards":tuple(type("Reward", (), reward)() for reward in rewards),"stone_cost":stone_cost})()
            finally:
                pass


__all__ = ["PuppetHarvestSqlRepository"]
