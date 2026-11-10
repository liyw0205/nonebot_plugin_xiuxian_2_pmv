from __future__ import annotations

from ...core.errors import DomainError

from pathlib import Path
from typing import Any

from ...infrastructure.database import DatabaseUnitOfWork


class NatalTrainingSqlRepository:
    OPERATION_TABLE = "natal_training_operations"
    OPERATION_COLUMNS = frozenset(
        {"operation_id", "user_id", "requested_exp", "base_cost", "growth_rate", "max_level",
            "max_exp_base", "max_exp_growth", "exp_added", "stone_cost", "level", "exp", "max_exp"}
    )

    def __init__(self, game_database: str | Path, player_database: str | Path) -> None:
        self.game_database = str(game_database)
        self.player_database = str(player_database)

    @classmethod
    def _operations_ready(cls, uow: DatabaseUnitOfWork) -> bool:
        """The ledger is created by startup migration ``natal_treasure.002``; requests never DDL."""
        if uow.query_one(
            "SELECT 1 AS present FROM sqlite_master WHERE type='table' AND name=?",
            (cls.OPERATION_TABLE,),
        ) is None:
            return False
        columns = {
            str(row["name"]).casefold()
            for row in uow.query_all(f'PRAGMA main.table_info("{cls.OPERATION_TABLE}")')
        }
        return cls.OPERATION_COLUMNS.issubset(columns)

    @staticmethod
    def _result(status: str, exp_added: int = 0, stone_cost: int = 0, level: int = 0, exp: int = 0, max_exp: int = 0, reason: str = "") -> dict[str, Any]:
        return {"reason": reason, "status": status, "exp_added": int(exp_added), "stone_cost": int(stone_cost), "level": int(level), "exp": int(exp), "max_exp": int(max_exp)}

    def train(self, operation_id: str, user_id: str, requested_exp: int, *, base_cost: int, growth_rate: float, max_level: int, max_exp_base: int, max_exp_growth: int) -> dict[str, Any]:
        operation_id, user_id = str(operation_id).strip(), str(user_id)
        requested_exp, base_cost, max_level, max_exp_base, max_exp_growth = map(int, (requested_exp, base_cost, max_level, max_exp_base, max_exp_growth))
        growth_rate = float(growth_rate)
        if not operation_id or requested_exp <= 0 or base_cost <= 0 or max_level <= 0:
            raise ValueError("valid operation and positive training parameters are required")
        with DatabaseUnitOfWork(self.game_database, immediate=True) as uow:
            uow.attach_database(self.player_database, "player_data")
            if not self._operations_ready(uow):
                return self._result("schema_missing")
            old = uow.query_one("SELECT user_id,requested_exp,exp_added,stone_cost,level,exp,max_exp FROM natal_training_operations WHERE operation_id=?", (operation_id,))
            if old is not None:
                return self._result("duplicate", old["exp_added"], old["stone_cost"], old["level"], old["exp"], old["max_exp"]) if str(old["user_id"]) == user_id and int(old["requested_exp"]) == requested_exp else self._result("state_changed", reason="operation_conflict")
            user = uow.query_one("SELECT COALESCE(stone,0) AS stone FROM user_xiuxian WHERE user_id=?", (user_id,))
            treasure = uow.query_one("SELECT COALESCE(form,0) AS form,COALESCE(level,0) AS level,COALESCE(exp,0) AS exp,COALESCE(max_exp,?) AS max_exp FROM player_data.natal_treasure WHERE user_id=?", (max_exp_base, user_id))
            if user is None:
                return self._result("user_missing")
            if treasure is None or int(treasure["form"]) == 0:
                return self._result("treasure_missing")
            level, current_exp, current_max_exp = int(treasure["level"]), int(treasure["exp"]), max_exp_base + int(treasure["level"]) * max_exp_growth
            if level >= max_level:
                return self._result("max_level", level=level, exp=current_exp, max_exp=current_max_exp)
            exp_added = min(requested_exp, max(0, current_max_exp - current_exp))
            if exp_added <= 0:
                return self._result("exp_full", level=level, exp=current_exp, max_exp=current_max_exp)
            stone_cost = int(base_cost * (1 + level * growth_rate)) * exp_added
            if int(user["stone"]) < stone_cost:
                return self._result("stone_insufficient", exp_added=exp_added, stone_cost=stone_cost, level=level, exp=current_exp, max_exp=current_max_exp)
            new_exp, new_level, new_max_exp = current_exp + exp_added, level, current_max_exp
            if new_exp >= current_max_exp:
                new_level += 1
                new_exp -= current_max_exp
                new_max_exp = max_exp_base + new_level * max_exp_growth if new_level < max_level else current_max_exp
            if uow.execute("UPDATE user_xiuxian SET stone=stone-? WHERE user_id=? AND stone>=?", (stone_cost, user_id, stone_cost)).rowcount != 1:
                raise DomainError("stone_write_rejected", "natal treasure write count mismatch")
            if uow.execute("UPDATE player_data.natal_treasure SET level=?,exp=?,max_exp=? WHERE user_id=?", (new_level, new_exp, new_max_exp, user_id)).rowcount != 1:
                raise DomainError("treasure_training_write_rejected", "natal treasure write count mismatch")
            uow.execute("INSERT INTO natal_training_operations VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?)", (operation_id, user_id, requested_exp, base_cost, growth_rate, max_level, max_exp_base, max_exp_growth, exp_added, stone_cost, new_level, new_exp, new_max_exp))
            return self._result("trained", exp_added, stone_cost, new_level, new_exp, new_max_exp)
