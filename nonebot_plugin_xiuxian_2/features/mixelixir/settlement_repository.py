from __future__ import annotations

import json
from pathlib import Path
from typing import Mapping

from ...infrastructure.database import DatabaseUnitOfWork


class MixelixirSettlementResult:
    def __init__(self, status: str, reward_quantity: int = 0) -> None:
        self.status = status
        self.reward_quantity = reward_quantity


class MixelixirSettlementSqlRepository:
    def __init__(self, database: str | Path) -> None:
        self.database = str(database)

    def settle(self, operation_id: str, user_id: str, materials: Mapping[int, int], reward_id: int, reward_name: str, reward_quantity: int, *, max_goods_num: int) -> MixelixirSettlementResult:
        operation_id, user_id = str(operation_id).strip(), str(user_id)
        reward_id, reward_quantity, max_goods_num = int(reward_id), int(reward_quantity), int(max_goods_num)
        normalized = {int(item_id): int(quantity) for item_id, quantity in dict(materials).items() if int(quantity) > 0}
        if not operation_id or not normalized or reward_quantity <= 0 or max_goods_num <= 0:
            raise ValueError("operation, materials, reward quantity and capacity are required")
        payload = json.dumps([user_id, sorted(normalized.items()), reward_id, reward_quantity], separators=(",", ":"))
        with DatabaseUnitOfWork(self.database, immediate=True) as uow:
            uow.execute("CREATE TABLE IF NOT EXISTS mixelixir_settlement_operations(operation_id TEXT PRIMARY KEY,payload TEXT NOT NULL,reward_quantity INTEGER NOT NULL,created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP)")
            previous = uow.query_one("SELECT payload,reward_quantity FROM mixelixir_settlement_operations WHERE operation_id=?", (operation_id,))
            if previous is not None:
                if str(previous["payload"]) != payload:
                    return MixelixirSettlementResult("state_changed")
                return MixelixirSettlementResult("duplicate", int(previous["reward_quantity"]))
            if uow.query_one("SELECT 1 FROM user_xiuxian WHERE user_id=?", (user_id,)) is None:
                return MixelixirSettlementResult("user_missing")
            for item_id, quantity in normalized.items():
                row = uow.query_one("SELECT COALESCE(goods_num,0) AS goods_num FROM back WHERE user_id=? AND goods_id=?", (user_id, item_id))
                if row is None or int(row["goods_num"]) < quantity:
                    return MixelixirSettlementResult("item_insufficient")
            for item_id, quantity in normalized.items():
                uow.execute("UPDATE back SET goods_num=goods_num-? WHERE user_id=? AND goods_id=? AND goods_num>=?", (quantity, user_id, item_id, quantity))
            existing = uow.query_one("SELECT goods_num FROM back WHERE user_id=? AND goods_id=?", (user_id, reward_id))
            if existing is not None and int(existing["goods_num"]) + reward_quantity > max_goods_num:
                return MixelixirSettlementResult("inventory_full")
            if existing is None:
                uow.execute("INSERT INTO back(user_id,goods_id,goods_name,goods_type,goods_num,bind_num) VALUES(?,?,?,?,?,0)", (user_id, reward_id, str(reward_name), "丹药", reward_quantity))
            else:
                uow.execute("UPDATE back SET goods_num=goods_num+? WHERE user_id=? AND goods_id=?", (reward_quantity, user_id, reward_id))
            if uow.execute("UPDATE user_xiuxian SET mixelixir_num=COALESCE(mixelixir_num,0)+1 WHERE user_id=?", (user_id,)).rowcount != 1:
                return MixelixirSettlementResult("state_changed")
            uow.execute("INSERT INTO mixelixir_settlement_operations(operation_id,payload,reward_quantity) VALUES(?,?,?)", (operation_id, payload, reward_quantity))
            return MixelixirSettlementResult("applied", reward_quantity)


__all__ = ["MixelixirSettlementSqlRepository", "MixelixirSettlementResult"]
