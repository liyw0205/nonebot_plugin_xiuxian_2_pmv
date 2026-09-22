from __future__ import annotations

import json
from pathlib import Path

from ...infrastructure.database import DatabaseUnitOfWork


class AdminStoneResult:
    def __init__(self, status: str, user_id: str, previous_stone: int = 0, final_stone: int = 0, applied_delta: int = 0) -> None:
        self.status = status
        self.user_id = user_id
        self.previous_stone = previous_stone
        self.final_stone = final_stone
        self.applied_delta = applied_delta


class AdminStoneSqlRepository:
    def __init__(self, database: str | Path) -> None:
        self.database = str(database)

    def adjust(self, operation_id: str, operator_id: str, user_id: str, expected_stone: int, requested_delta: int, **kwargs) -> AdminStoneResult:
        payload = json.dumps([operator_id, user_id], separators=(",", ":"))
        with DatabaseUnitOfWork(self.database, immediate=True) as uow:
            uow.execute("CREATE TABLE IF NOT EXISTS admin_stone_operations(operation_id TEXT PRIMARY KEY,payload TEXT NOT NULL,user_id TEXT NOT NULL,previous_stone INTEGER NOT NULL,final_stone INTEGER NOT NULL,applied_delta INTEGER NOT NULL,created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP)")
            previous = uow.query_one("SELECT payload,user_id,previous_stone,final_stone,applied_delta FROM admin_stone_operations WHERE operation_id=?", (operation_id,))
            if previous is not None:
                if str(previous["payload"]) != payload:
                    return AdminStoneResult("state_changed", str(previous["user_id"]))
                return AdminStoneResult("duplicate", str(previous["user_id"]), int(previous["previous_stone"]), int(previous["final_stone"]), int(previous["applied_delta"]))
            row = uow.query_one("SELECT COALESCE(stone,0) AS stone FROM user_xiuxian WHERE user_id=?", (user_id,))
            if row is None:
                return AdminStoneResult("user_missing", user_id)
            current = int(row["stone"])
            if current != int(expected_stone):
                return AdminStoneResult("state_changed", user_id, current, current, 0)
            final = current + int(requested_delta)
            if final < 0:
                return AdminStoneResult("insufficient_stone", user_id, current, current, 0)
            uow.execute("UPDATE user_xiuxian SET stone=? WHERE user_id=? AND stone=?", (final, user_id, current))
            uow.execute("INSERT INTO admin_stone_operations(operation_id,payload,user_id,previous_stone,final_stone,applied_delta) VALUES(?,?,?,?,?,?)", (operation_id, payload, user_id, current, final, int(requested_delta)))
            return AdminStoneResult("applied", user_id, current, final, int(requested_delta))


__all__ = ["AdminStoneSqlRepository", "AdminStoneResult"]
