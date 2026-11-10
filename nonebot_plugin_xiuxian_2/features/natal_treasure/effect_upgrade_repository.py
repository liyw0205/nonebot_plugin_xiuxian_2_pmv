from __future__ import annotations

from ...core.errors import DomainError

import random
from pathlib import Path
from typing import Any

from ...infrastructure.database import DatabaseUnitOfWork


class NatalEffectUpgradeSqlRepository:
    OPERATION_TABLE = "natal_effect_upgrade_operations"
    OPERATION_COLUMNS = frozenset(
        {"operation_id", "user_id", "scripture_id", "scripture_cost", "max_slots",
            "max_effect_level", "choice_seed", "slot", "effect_type", "level"}
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
    def _result(status: str, slot: int = 0, effect_type: int = 0, level: int = 0, reason: str = "") -> dict[str, Any]:
        return {"reason": reason, "status": status, "slot": int(slot), "effect_type": int(effect_type), "level": int(level)}

    def upgrade(self, operation_id: str, user_id: str, scripture_id: int, scripture_cost: int, max_slots: int, max_effect_level: int, choice_seed: int) -> dict[str, Any]:
        operation_id, user_id = str(operation_id).strip(), str(user_id)
        scripture_id, scripture_cost, max_slots, max_effect_level, choice_seed = map(int, (scripture_id, scripture_cost, max_slots, max_effect_level, choice_seed))
        if not operation_id or scripture_cost < 0 or max_slots <= 0 or max_effect_level <= 0:
            raise ValueError("valid effect upgrade parameters are required")
        with DatabaseUnitOfWork(self.game_database, immediate=True) as uow:
            uow.attach_database(self.player_database, "player_data")
            if not self._operations_ready(uow):
                return self._result("schema_missing")
            old = uow.query_one("SELECT user_id,scripture_id,scripture_cost,slot,effect_type,level FROM natal_effect_upgrade_operations WHERE operation_id=?", (operation_id,))
            if old is not None:
                return self._result("duplicate", old["slot"], old["effect_type"], old["level"]) if str(old["user_id"]) == user_id and int(old["scripture_id"]) == scripture_id and int(old["scripture_cost"]) == scripture_cost else self._result("state_changed", reason="operation_conflict")
            columns = {str(row["name"]) for row in uow.query_all("PRAGMA player_data.table_info(natal_treasure)")}
            required = {"form"} | {f"effect{slot}_{field}" for slot in range(1, max_slots + 1) for field in ("type", "level")}
            if not required.issubset(columns):
                return self._result("treasure_missing", reason="schema_missing")
            treasure = uow.query_one("SELECT form FROM player_data.natal_treasure WHERE user_id=?", (user_id,))
            if treasure is None or int(treasure["form"] or 0) == 0:
                return self._result("treasure_missing")
            effects = []
            for slot in range(1, max_slots + 1):
                row = uow.query_one(f"SELECT effect{slot}_type AS effect_type,effect{slot}_level AS level FROM player_data.natal_treasure WHERE user_id=?", (user_id,))
                if row and int(row["effect_type"] or 0) and int(row["level"] or 0) < max_effect_level:
                    effects.append((int(row["level"] or 0), slot, int(row["effect_type"])))
            if not effects:
                return self._result("max_level")
            _, slot, effect_type = min(effects)
            item = uow.query_one("SELECT goods_num FROM back WHERE user_id=? AND goods_id=?", (user_id, scripture_id))
            if item is None or int(item["goods_num"] or 0) < scripture_cost:
                return self._result("item_insufficient")
            level = min(max_effect_level, min(effects)[0] + 1)
            if uow.execute("UPDATE back SET goods_num=goods_num-? WHERE user_id=? AND goods_id=? AND goods_num>=?", (scripture_cost, user_id, scripture_id, scripture_cost)).rowcount != 1:
                raise DomainError("item_write_rejected", "natal treasure write count mismatch")
            if uow.execute(f"UPDATE player_data.natal_treasure SET effect{slot}_level=? WHERE user_id=?", (level, user_id)).rowcount != 1:
                raise DomainError("treasure_effect_write_rejected", "natal treasure write count mismatch")
            uow.execute("INSERT INTO natal_effect_upgrade_operations VALUES(?,?,?,?,?,?,?,?,?,?)", (operation_id, user_id, scripture_id, scripture_cost, max_slots, max_effect_level, choice_seed, slot, effect_type, level))
            return self._result("upgraded", slot, effect_type, level)
