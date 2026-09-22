from __future__ import annotations

import json
from dataclasses import dataclass
from pathlib import Path

from ...infrastructure.database import DatabaseUnitOfWork


@dataclass(frozen=True)
class DufangBetResult:
    status: str
    cost: int = 0
    wallet_stone: int = 0
    bet_id: str = ""

    @property
    def succeeded(self) -> bool:
        return self.status in {"applied", "duplicate"}


class DufangBetSqlRepository:
    def __init__(self, game_database: str | Path, player_database: str | Path) -> None:
        self.game_database, self.player_database = str(game_database), str(player_database)

    def place(self, operation_id: str, user_id: str, cost: int, placed_at: str) -> DufangBetResult:
        operation_id, user_id, cost = str(operation_id).strip(), str(user_id), int(cost)
        placed_at = str(placed_at).strip()
        if not operation_id or cost <= 0 or not placed_at:
            raise ValueError("operation id, positive cost and placement time are required")
        payload = json.dumps([user_id, cost], ensure_ascii=True, separators=(",", ":"))
        with DatabaseUnitOfWork(self.game_database, immediate=True) as uow:
            uow.execute("ATTACH DATABASE ? AS player_data", (self.player_database,))
            uow.execute("CREATE TABLE IF NOT EXISTS dufang_bets(bet_id TEXT PRIMARY KEY,user_id TEXT NOT NULL,cost INTEGER NOT NULL,status TEXT NOT NULL,placed_at TEXT NOT NULL,settled_at TEXT)")
            uow.execute("CREATE TABLE IF NOT EXISTS dufang_bet_operations(operation_id TEXT PRIMARY KEY,payload TEXT NOT NULL,cost INTEGER NOT NULL,wallet_stone INTEGER NOT NULL,bet_id TEXT NOT NULL,created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP)")
            previous = uow.query_one("SELECT payload,cost,wallet_stone,bet_id FROM dufang_bet_operations WHERE operation_id=?", (operation_id,))
            if previous is not None:
                if str(previous["payload"]) != payload:
                    return DufangBetResult("state_changed")
                return DufangBetResult("duplicate", int(previous["cost"]), int(previous["wallet_stone"]), str(previous["bet_id"]))
            user = uow.query_one("SELECT COALESCE(stone,0) AS stone FROM user_xiuxian WHERE user_id=?", (user_id,))
            if user is None:
                return DufangBetResult("user_missing")
            wallet = int(user["stone"])
            if wallet < cost:
                return DufangBetResult("stone_insufficient", wallet_stone=wallet)
            uow.execute("CREATE TABLE IF NOT EXISTS player_data.unseal_data(user_id TEXT PRIMARY KEY,count INTEGER,total_cost INTEGER,profit INTEGER,loss INTEGER,shared_profit INTEGER,shared_loss INTEGER,received_profit INTEGER,received_loss INTEGER,last_update TEXT)")
            if uow.execute("UPDATE user_xiuxian SET stone=stone-? WHERE user_id=? AND stone>=?", (cost, user_id, cost)).rowcount != 1:
                return DufangBetResult("state_changed", wallet_stone=wallet)
            uow.execute("INSERT INTO player_data.unseal_data(user_id,count,total_cost,last_update) VALUES(?,?,?,?) ON CONFLICT(user_id) DO UPDATE SET count=COALESCE(count,0)+1,total_cost=COALESCE(total_cost,0)+excluded.total_cost,last_update=excluded.last_update", (user_id, 1, cost, placed_at))
            uow.execute("INSERT INTO dufang_bets VALUES(?,?,?,?,?,NULL)", (operation_id, user_id, cost, "pending", placed_at))
            uow.execute("INSERT INTO dufang_bet_operations VALUES(?,?,?,?,?,CURRENT_TIMESTAMP)", (operation_id, payload, cost, wallet - cost, operation_id))
            return DufangBetResult("applied", cost, wallet - cost, operation_id)


__all__ = ["DufangBetSqlRepository", "DufangBetResult"]
