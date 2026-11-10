from __future__ import annotations

from ...core.errors import DomainError

import json
import random
from pathlib import Path
from typing import Any

from ...infrastructure.database import DatabaseUnitOfWork


class NatalAwakenSqlRepository:
    OPERATION_TABLE = "natal_awaken_operations"
    OPERATION_COLUMNS = frozenset(
        {"operation_id", "user_id", "max_slots", "choice_seed", "form", "name", "effect_type", "base_value"}
    )

    def __init__(self, player_database: str | Path) -> None:
        self.player_database = str(player_database)

    @classmethod
    def _operations_ready(cls, uow: DatabaseUnitOfWork) -> bool:
        """The ledger is created by startup migration ``natal_treasure.003``; requests never DDL."""
        if uow.query_one(
            "SELECT 1 AS present FROM sqlite_master WHERE type='table' AND name=?",
            (cls.OPERATION_TABLE,),
        ) is None:
            return False
        columns = {
            str(row["name"]).casefold()
            for row in uow.query_all(f'PRAGMA table_info("{cls.OPERATION_TABLE}")')
        }
        return cls.OPERATION_COLUMNS.issubset(columns)

    @staticmethod
    def _result(status: str, form: int = 0, name: str = "", effect_type: int = 0, base_value: float = 0.0, reason: str = "") -> dict[str, Any]:
        return {"reason": reason, "status": status, "form": int(form), "name": str(name), "effect_type": int(effect_type), "base_value": float(base_value)}

    def get_result(self, operation_id: str) -> dict[str, Any] | None:
        with DatabaseUnitOfWork(self.player_database) as uow:
            row = uow.query_one("SELECT form,name,effect_type,base_value FROM natal_awaken_operations WHERE operation_id=?", (str(operation_id).strip(),))
        if row is None:
            return None
        return self._result("duplicate", row["form"], row["name"], row["effect_type"], row["base_value"])

    def awaken(self, operation_id: str, user_id: str, max_slots: int, effect_configs: dict[str, Any], effect_names: dict[str, Any], fixed_base_effects: set[str], choice_seed: int) -> dict[str, Any]:
        operation_id, user_id, max_slots, choice_seed = str(operation_id).strip(), str(user_id), int(max_slots), int(choice_seed)
        configs = {int(key): (float(value[0]), float(value[1])) for key, value in effect_configs.items()}
        names = {int(key): tuple(str(name) for name in value) for key, value in effect_names.items()}
        fixed = {int(value) for value in fixed_base_effects}
        if not operation_id or max_slots <= 0 or not configs or any(not names.get(key) for key in configs):
            raise ValueError("valid operation and awaken parameters are required")
        with DatabaseUnitOfWork(self.player_database, immediate=True) as uow:
            if not self._operations_ready(uow):
                return self._result("schema_missing")
            previous = uow.query_one("SELECT user_id,max_slots,choice_seed,form,name,effect_type,base_value FROM natal_awaken_operations WHERE operation_id=?", (operation_id,))
            if previous is not None:
                if str(previous["user_id"]) != user_id:
                    return self._result("state_changed", reason="operation_conflict")
                return self._result("duplicate", previous["form"], previous["name"], previous["effect_type"], previous["base_value"])
            required = {"form", "name", "level", "exp", "max_exp", "fate_revive_count", "immortal_revive_count", "invincible_gain_count", "nirvana_revive_count", "soul_return_revive_count", "charge_status", "soul_summon_count", "enlightenment_count"}
            for slot in range(1, max_slots + 1):
                required.update((f"effect{slot}_type", f"effect{slot}_base_value", f"effect{slot}_level"))
            table = uow.query_one("SELECT 1 AS present FROM sqlite_master WHERE type='table' AND name='natal_treasure'")
            if table is None:
                return self._result("treasure_missing", reason="schema_missing")
            columns = {str(row["name"]) for row in uow.query_all("PRAGMA table_info(natal_treasure)")}
            if not required.issubset(columns):
                return self._result("treasure_missing", reason="schema_missing")
            current = uow.query_one("SELECT form FROM natal_treasure WHERE user_id=?", (user_id,))
            if current is None:
                return self._result("treasure_missing")
            if int(current["form"] or 0) != 0:
                return self._result("already_awakened")
            rng = random.Random(choice_seed)
            form = rng.randint(1, 4)
            effect_type = rng.choice(sorted(configs))
            name = rng.choice(names[effect_type])
            minimum, maximum = configs[effect_type]
            base_value = minimum if effect_type in fixed else round(rng.uniform(minimum, maximum), 3)
            assignments = [("form", form), ("name", name), ("level", 0), ("exp", 0), ("max_exp", 100), ("fate_revive_count", 0), ("immortal_revive_count", 0), ("invincible_gain_count", 0), ("nirvana_revive_count", 0), ("soul_return_revive_count", 0), ("charge_status", 0), ("soul_summon_count", "{}"), ("enlightenment_count", "{}")]
            for slot in range(1, max_slots + 1):
                assignments.extend(((f"effect{slot}_type", effect_type if slot == 1 else 0), (f"effect{slot}_base_value", base_value if slot == 1 else 0.0), (f"effect{slot}_level", 1 if slot == 1 else 0)))
            updated = uow.execute("UPDATE natal_treasure SET " + ", ".join(f'"{field}"=?' for field, _ in assignments) + " WHERE user_id=? AND COALESCE(form,0)=0", tuple(value for _, value in assignments) + (user_id,))
            if updated.rowcount != 1:
                raise DomainError("treasure_awaken_write_rejected", "natal treasure write count mismatch")
            uow.execute("INSERT INTO natal_awaken_operations(operation_id,user_id,max_slots,choice_seed,form,name,effect_type,base_value) VALUES(?,?,?,?,?,?,?,?)", (operation_id, user_id, max_slots, choice_seed, form, name, effect_type, base_value))
            return self._result("awakened", form, name, effect_type, base_value)


__all__ = ["NatalAwakenSqlRepository"]
