from __future__ import annotations

import random
from pathlib import Path
from typing import Any

from ...infrastructure.database import DatabaseUnitOfWork


class NatalEngravingSqlRepository:
    def __init__(self, game_database: str | Path, player_database: str | Path) -> None:
        self.game_database = str(game_database); self.player_database = str(player_database)

    @staticmethod
    def _result(status: str, slot: int = 0, effect_type: int = 0, base_value: float = 0.0) -> dict[str, Any]:
        return {"status": status, "slot": int(slot), "effect_type": int(effect_type), "base_value": float(base_value)}

    def engrave(self, operation_id: str, user_id: str, scripture_id: int, scripture_cost: int, max_slots: int, effect_configs: dict[str, Any], fixed_base_effects: set[str], choice_seed: int) -> dict[str, Any]:
        operation_id, user_id = str(operation_id).strip(), str(user_id)
        scripture_id, scripture_cost, max_slots, choice_seed = map(int, (scripture_id, scripture_cost, max_slots, choice_seed))
        configs = {int(key): (float(value[0]), float(value[1])) for key, value in effect_configs.items()}
        fixed = {int(value) for value in fixed_base_effects}
        with DatabaseUnitOfWork(self.game_database, immediate=True) as uow:
            uow.attach_database(self.player_database, "player_data")
            uow.execute("CREATE TABLE IF NOT EXISTS natal_engraving_operations(operation_id TEXT PRIMARY KEY,user_id TEXT,scripture_id INTEGER,scripture_cost INTEGER,max_slots INTEGER,choice_seed INTEGER,slot INTEGER,effect_type INTEGER,base_value REAL)")
            old = uow.query_one("SELECT user_id,scripture_id,scripture_cost,slot,effect_type,base_value FROM natal_engraving_operations WHERE operation_id=?", (operation_id,))
            if old is not None:
                return self._result("duplicate", old["slot"], old["effect_type"], old["base_value"]) if str(old["user_id"]) == user_id else self._result("state_changed")
            treasure = uow.query_one("SELECT form FROM player_data.natal_treasure WHERE user_id=?", (user_id,))
            if treasure is None or int(treasure["form"] or 0) == 0:
                return self._result("treasure_missing")
            empty = None
            for slot in range(1, max_slots + 1):
                row = uow.query_one(f"SELECT effect{slot}_type AS effect_type FROM player_data.natal_treasure WHERE user_id=?", (user_id,))
                if row and not int(row["effect_type"] or 0): empty = slot; break
            if empty is None: return self._result("slots_full")
            item = uow.query_one("SELECT goods_num FROM back WHERE user_id=? AND goods_id=?", (user_id, scripture_id))
            if item is None or int(item["goods_num"] or 0) < scripture_cost: return self._result("item_insufficient")
            rng = random.Random(choice_seed); effect_type = rng.choice(sorted(configs)); minimum, maximum = configs[effect_type]; base_value = minimum if effect_type in fixed else round(rng.uniform(minimum, maximum), 3)
            if uow.execute("UPDATE back SET goods_num=goods_num-? WHERE user_id=? AND goods_id=? AND goods_num>=?", (scripture_cost, user_id, scripture_id, scripture_cost)).rowcount != 1: return self._result("state_changed")
            if uow.execute(f"UPDATE player_data.natal_treasure SET effect{empty}_type=?,effect{empty}_base_value=?,effect{empty}_level=1 WHERE user_id=?", (effect_type, base_value, user_id)).rowcount != 1: return self._result("state_changed")
            uow.execute("INSERT INTO natal_engraving_operations VALUES(?,?,?,?,?,?,?,?,?)", (operation_id,user_id,scripture_id,scripture_cost,max_slots,choice_seed,empty,effect_type,base_value))
            return self._result("engraved", empty, effect_type, base_value)
