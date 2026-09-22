from __future__ import annotations

import json
from pathlib import Path

from ...infrastructure.database import DatabaseUnitOfWork


class MixelixirHarvestLevelUpgradeResult:
    def __init__(self, status: str, cost: int = 0, wallet_stone: int = 0, level: int = 0, experience: int = 0) -> None:
        self.status = status
        self.cost = cost
        self.wallet_stone = wallet_stone
        self.level = level
        self.experience = experience


class MixelixirHarvestLevelUpgradeSqlRepository:
    def __init__(self, game_database: str | Path, player_database: str | Path) -> None:
        self.game_database = str(game_database)
        self.player_database = str(player_database)

    def upgrade(self, operation_id: str, user_id: str, expected_level: int, expected_experience: int, expected_stone: int | None, next_level: int, cost: int) -> MixelixirHarvestLevelUpgradeResult:
        operation_id, user_id = str(operation_id).strip(), str(user_id)
        expected_level, expected_experience, next_level, cost = map(int, (expected_level, expected_experience, next_level, cost))
        if expected_stone is not None:
            expected_stone = int(expected_stone)
        if not operation_id or min(expected_level, expected_experience) < 0 or (expected_stone is not None and expected_stone < 0) or next_level != expected_level + 1 or cost <= 0:
            raise ValueError("valid operation, state snapshot, next level and cost are required")
        payload = json.dumps([user_id, next_level, cost], separators=(",", ":"))
        with DatabaseUnitOfWork(self.game_database, immediate=True) as uow:
            uow.execute("ATTACH DATABASE ? AS player_data", (self.player_database,))
            try:
                uow.execute("CREATE TABLE IF NOT EXISTS mixelixir_harvest_level_upgrade_operations(operation_id TEXT PRIMARY KEY,payload TEXT NOT NULL,cost INTEGER NOT NULL,wallet_stone INTEGER NOT NULL,level INTEGER NOT NULL,experience INTEGER NOT NULL,created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP)")
                previous = uow.query_one("SELECT payload,cost,wallet_stone,level,experience FROM mixelixir_harvest_level_upgrade_operations WHERE operation_id=?", (operation_id,))
                if previous is not None:
                    if str(previous["payload"]) != payload:
                        return MixelixirHarvestLevelUpgradeResult("state_changed")
                    return MixelixirHarvestLevelUpgradeResult("duplicate", int(previous["cost"]), int(previous["wallet_stone"]), int(previous["level"]), int(previous["experience"]))
                row = uow.query_one("SELECT COALESCE(stone,0) AS stone FROM user_xiuxian WHERE user_id=?", (user_id,))
                state = uow.query_one("SELECT CAST(COALESCE(\"收取等级\",'0') AS INTEGER) AS level,CAST(COALESCE(\"炼丹经验\",'0') AS INTEGER) AS experience FROM player_data.mix_elixir_info WHERE user_id=?", (user_id,))
                if row is None or state is None:
                    return MixelixirHarvestLevelUpgradeResult("state_changed")
                current_level, current_exp, current_stone = int(state["level"]), int(state["experience"]), int(row["stone"])
                if (current_level, current_exp) != (expected_level, expected_experience) or (expected_stone is not None and current_stone != expected_stone):
                    return MixelixirHarvestLevelUpgradeResult("state_changed", wallet_stone=current_stone, level=current_level, experience=current_exp)
                if current_exp < cost:
                    return MixelixirHarvestLevelUpgradeResult("experience_insufficient", wallet_stone=current_stone, level=current_level, experience=current_exp)
                remaining = current_exp - cost
                changed = uow.execute("UPDATE player_data.mix_elixir_info SET \"收取等级\"=?,\"炼丹经验\"=? WHERE user_id=? AND CAST(\"收取等级\" AS INTEGER)=? AND CAST(\"炼丹经验\" AS INTEGER)=?", (str(next_level), str(remaining), user_id, expected_level, expected_experience))
                if changed.rowcount != 1:
                    return MixelixirHarvestLevelUpgradeResult("state_changed")
                uow.execute("INSERT INTO mixelixir_harvest_level_upgrade_operations(operation_id,payload,cost,wallet_stone,level,experience) VALUES(?,?,?,?,?,?)", (operation_id, payload, cost, current_stone, next_level, remaining))
                return MixelixirHarvestLevelUpgradeResult("applied", cost, current_stone, next_level, remaining)
            finally:
                pass


__all__ = ["MixelixirHarvestLevelUpgradeSqlRepository", "MixelixirHarvestLevelUpgradeResult"]
