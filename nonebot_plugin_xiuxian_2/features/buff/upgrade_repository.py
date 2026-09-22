from __future__ import annotations

import json
from pathlib import Path

from ...infrastructure.database import DatabaseUnitOfWork


class BlessedSpotUpgradeResult:
    def __init__(self, status: str, user_id: str, stone_cost: int = 0, previous_level: int = 0, current_level: int = 0) -> None:
        self.status = status
        self.user_id = user_id
        self.stone_cost = stone_cost
        self.previous_level = previous_level
        self.current_level = current_level


class BlessedSpotUpgradeSqlRepository:
    def __init__(self, game_database: str | Path, player_database: str | Path) -> None:
        self.game_database = str(game_database)
        self.player_database = str(player_database)

    def upgrade(self, operation_id: str, user_id: str, expected_level: int, stone_cost: int, max_level: int = 10) -> BlessedSpotUpgradeResult:
        operation_id, user_id = str(operation_id).strip(), str(user_id)
        expected_level, stone_cost, max_level = int(expected_level), int(stone_cost), int(max_level)
        if not operation_id or expected_level <= 0 or stone_cost <= 0 or max_level <= 1:
            raise ValueError("valid operation, level and cost are required")
        payload = json.dumps([user_id, expected_level, stone_cost, max_level], separators=(",", ":"))
        with DatabaseUnitOfWork(self.game_database, immediate=True) as uow:
            uow.execute("ATTACH DATABASE ? AS player_data", (self.player_database,))
            try:
                uow.execute("CREATE TABLE IF NOT EXISTS blessed_spot_operations(operation_id TEXT PRIMARY KEY,action TEXT NOT NULL,payload TEXT NOT NULL,result_json TEXT NOT NULL,created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP)")
                previous = uow.query_one("SELECT payload,result_json FROM blessed_spot_operations WHERE operation_id=? AND action=?", (operation_id, "upgrade"))
                if previous is not None:
                    if str(previous["payload"]) != payload:
                        return BlessedSpotUpgradeResult("state_changed", user_id)
                    saved = json.loads(str(previous["result_json"]))
                    return BlessedSpotUpgradeResult("duplicate", user_id, saved["stone_cost"], saved["previous_level"], saved["current_level"])
                user = uow.query_one("SELECT COALESCE(stone,0) AS stone,COALESCE(blessed_spot_flag,0) AS enabled FROM user_xiuxian WHERE user_id=?", (user_id,))
                level = uow.query_one("SELECT COALESCE(\"灵田数量\",'0') AS level FROM player_data.mix_elixir_info WHERE user_id=?", (user_id,))
                if user is None or level is None or int(user["enabled"]) == 0:
                    return BlessedSpotUpgradeResult("blessed_spot_missing", user_id)
                current = int(level["level"] or 0)
                if current != expected_level:
                    return BlessedSpotUpgradeResult("state_changed", user_id, previous_level=current, current_level=current)
                if current >= max_level:
                    return BlessedSpotUpgradeResult("max_level", user_id, previous_level=current, current_level=current)
                if int(user["stone"]) < stone_cost:
                    return BlessedSpotUpgradeResult("stone_insufficient", user_id, stone_cost, current, current)
                uow.execute("UPDATE user_xiuxian SET stone=stone-? WHERE user_id=? AND stone>=?", (stone_cost, user_id, stone_cost))
                changed = uow.execute("UPDATE player_data.mix_elixir_info SET \"灵田数量\"=? WHERE user_id=? AND CAST(\"灵田数量\" AS INTEGER)=?", (str(current + 1), user_id, current))
                if changed.rowcount != 1:
                    return BlessedSpotUpgradeResult("state_changed", user_id)
                saved = {"stone_cost": stone_cost, "previous_level": current, "current_level": current + 1}
                uow.execute("INSERT INTO blessed_spot_operations(operation_id,action,payload,result_json) VALUES(?,?,?,?)", (operation_id, "upgrade", payload, json.dumps(saved)))
                return BlessedSpotUpgradeResult("applied", user_id, stone_cost, current, current + 1)
            finally:
                # DatabaseUnitOfWork closes the connection after commit/rollback; explicit DETACH here would run before the transaction is finalized.
                pass


__all__ = ["BlessedSpotUpgradeSqlRepository", "BlessedSpotUpgradeResult"]
