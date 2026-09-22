from __future__ import annotations

import json
from pathlib import Path
from typing import Any, Mapping, Sequence

from ...infrastructure.database import DatabaseUnitOfWork


class MixelixirHarvestResult:
    def __init__(self, status: str, harvested_at: str, rewards: Sequence[Mapping[str, Any]] = ()) -> None:
        self.status = status
        self.harvested_at = harvested_at
        self.rewards = tuple(dict(item) for item in rewards)


class MixelixirHarvestSqlRepository:
    def __init__(self, game_database: str | Path, player_database: str | Path) -> None:
        self.game_database = str(game_database)
        self.player_database = str(player_database)

    def harvest(self, operation_id: str, user_id: str, expected_last_time: str, harvested_at: str, rewards: Sequence[Mapping[str, Any]], *, max_goods_num: int) -> MixelixirHarvestResult:
        operation_id, user_id = str(operation_id).strip(), str(user_id)
        normalized = tuple({"item_id": int(item["item_id"]), "name": str(item["name"]), "quantity": int(item["quantity"])} for item in rewards)
        payload = json.dumps([user_id], ensure_ascii=False, separators=(",", ":"))
        with DatabaseUnitOfWork(self.game_database, immediate=True) as uow:
            uow.execute("ATTACH DATABASE ? AS player_data", (self.player_database,))
            try:
                uow.execute("CREATE TABLE IF NOT EXISTS mixelixir_harvest_operations(operation_id TEXT PRIMARY KEY,payload TEXT NOT NULL,harvested_at TEXT NOT NULL,rewards_json TEXT NOT NULL,created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP)")
                previous = uow.query_one("SELECT payload,harvested_at,rewards_json FROM mixelixir_harvest_operations WHERE operation_id=?", (operation_id,))
                if previous is not None:
                    if str(previous["payload"]) != payload:
                        return MixelixirHarvestResult("state_changed", "", ())
                    return MixelixirHarvestResult("duplicate", str(previous["harvested_at"]), json.loads(str(previous["rewards_json"])))
                row = uow.query_one("SELECT COALESCE(\"收取时间\",'') AS last_time FROM player_data.mix_elixir_info WHERE user_id=?", (user_id,))
                if row is None:
                    return MixelixirHarvestResult("user_missing", harvested_at, ())
                if str(row["last_time"]) != expected_last_time:
                    return MixelixirHarvestResult("state_changed", harvested_at, ())
                for item in normalized:
                    existing = uow.query_one("SELECT COALESCE(goods_num,0) AS goods_num FROM back WHERE user_id=? AND goods_id=?", (user_id, item["item_id"]))
                    if existing is not None and int(existing["goods_num"]) + item["quantity"] > int(max_goods_num):
                        return MixelixirHarvestResult("inventory_full", harvested_at, ())
                for item in normalized:
                    existing = uow.query_one("SELECT goods_num FROM back WHERE user_id=? AND goods_id=?", (user_id, item["item_id"]))
                    if existing is None:
                        uow.execute("INSERT INTO back(user_id,goods_id,goods_name,goods_type,goods_num,bind_num) VALUES(?,?,?,?,?,0)", (user_id, item["item_id"], item["name"], "药材", item["quantity"]))
                    else:
                        uow.execute("UPDATE back SET goods_num=COALESCE(goods_num,0)+? WHERE user_id=? AND goods_id=?", (item["quantity"], user_id, item["item_id"]))
                changed = uow.execute("UPDATE player_data.mix_elixir_info SET \"收取时间\"=? WHERE user_id=? AND COALESCE(\"收取时间\",'')=?", (harvested_at, user_id, expected_last_time))
                if changed.rowcount != 1:
                    return MixelixirHarvestResult("state_changed", harvested_at, ())
                rewards_json = json.dumps(normalized, ensure_ascii=False)
                uow.execute("INSERT INTO mixelixir_harvest_operations(operation_id,payload,harvested_at,rewards_json) VALUES(?,?,?,?)", (operation_id, payload, harvested_at, rewards_json))
                return MixelixirHarvestResult("applied", harvested_at, normalized)
            finally:
                pass


__all__ = ["MixelixirHarvestSqlRepository", "MixelixirHarvestResult"]
