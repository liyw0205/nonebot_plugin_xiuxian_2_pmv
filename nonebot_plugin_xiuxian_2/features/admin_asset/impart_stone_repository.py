from __future__ import annotations

import json
from pathlib import Path

from ...infrastructure.database import DatabaseUnitOfWork


class AdminImpartStoneResult:
    def __init__(self, status: str, previous_stone: int = 0, final_stone: int = 0, applied_delta: int = 0) -> None:
        self.status = status
        self.previous_stone = previous_stone
        self.final_stone = final_stone
        self.applied_delta = applied_delta


class AdminImpartStoneSqlRepository:
    def __init__(self, game_database: str | Path, impart_database: str | Path) -> None:
        self.game_database = str(game_database)
        self.impart_database = str(impart_database)

    def adjust(self, operation_id: str, operator_id: str, user_id: str, expected_stone: int, requested_delta: int, **kwargs) -> AdminImpartStoneResult:
        payload = json.dumps([operator_id, user_id], separators=(",", ":"))
        with DatabaseUnitOfWork(self.game_database, immediate=True) as uow:
            uow.execute("ATTACH DATABASE ? AS impart_data", (self.impart_database,))
            try:
                uow.execute("CREATE TABLE IF NOT EXISTS admin_impart_stone_operations(operation_id TEXT PRIMARY KEY,payload TEXT NOT NULL,previous_stone INTEGER NOT NULL,final_stone INTEGER NOT NULL,applied_delta INTEGER NOT NULL,created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP)")
                previous = uow.query_one("SELECT payload,previous_stone,final_stone,applied_delta FROM admin_impart_stone_operations WHERE operation_id=?", (operation_id,))
                if previous is not None:
                    if str(previous["payload"]) != payload:
                        return AdminImpartStoneResult("state_changed")
                    return AdminImpartStoneResult("duplicate", int(previous["previous_stone"]), int(previous["final_stone"]), int(previous["applied_delta"]))
                row = uow.query_one("SELECT COALESCE(stone,0) AS stone FROM user_xiuxian WHERE user_id=?", (user_id,))
                if row is None:
                    return AdminImpartStoneResult("user_missing")
                current = int(row["stone"])
                if current != int(expected_stone):
                    return AdminImpartStoneResult("state_changed", current, current, 0)
                final = current + int(requested_delta)
                if final < 0:
                    return AdminImpartStoneResult("insufficient_stone", current, current, 0)
                uow.execute("UPDATE user_xiuxian SET stone=? WHERE user_id=? AND stone=?", (final, user_id, current))
                uow.execute("CREATE TABLE IF NOT EXISTS impart_data.statistics(user_id TEXT PRIMARY KEY,stone INTEGER)")
                uow.execute("INSERT INTO impart_data.statistics(user_id,stone) VALUES(?,?) ON CONFLICT(user_id) DO UPDATE SET stone=impart_data.statistics.stone+excluded.stone", (user_id, int(requested_delta)))
                uow.execute("INSERT INTO admin_impart_stone_operations(operation_id,payload,previous_stone,final_stone,applied_delta) VALUES(?,?,?,?,?)", (operation_id, payload, current, final, int(requested_delta)))
                return AdminImpartStoneResult("applied", current, final, int(requested_delta))
            finally:
                pass


__all__ = ["AdminImpartStoneSqlRepository", "AdminImpartStoneResult"]
