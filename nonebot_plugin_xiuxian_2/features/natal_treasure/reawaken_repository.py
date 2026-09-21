from __future__ import annotations

import random
from pathlib import Path
from typing import Any

from ...infrastructure.database import DatabaseUnitOfWork


class NatalReawakenSqlRepository:
    def __init__(self, game_database: str | Path, player_database: str | Path) -> None:
        self.game_database = str(game_database)
        self.player_database = str(player_database)

    @staticmethod
    def _result(status: str, form: int = 0, name: str = "", effect_type: int = 0, base_value: float = 0.0, scripture_change: int = 0) -> dict[str, Any]:
        return {"status": status, "form": int(form), "name": str(name), "effect_type": int(effect_type), "base_value": float(base_value), "scripture_change": int(scripture_change)}

    def reawaken(self, operation_id: str, user_id: str, scripture_id: int, scripture_name: str, scripture_type: str, scripture_cost: int, max_slots: int, max_goods_num: int, effect_configs: dict[str, Any], effect_names: dict[str, Any], fixed_base_effects: set[str], choice_seed: int) -> dict[str, Any]:
        operation_id, user_id = str(operation_id).strip(), str(user_id)
        scripture_id, scripture_cost, max_slots, max_goods_num, choice_seed = map(int, (scripture_id, scripture_cost, max_slots, max_goods_num, choice_seed))
        configs = {int(key): (float(value[0]), float(value[1])) for key, value in effect_configs.items()}
        names = {int(key): tuple(str(name) for name in value) for key, value in effect_names.items()}
        fixed = {int(value) for value in fixed_base_effects}
        if not operation_id or scripture_cost < 0 or max_slots <= 0 or max_goods_num <= 0 or not configs:
            raise ValueError("valid operation and reawaken parameters are required")
        with DatabaseUnitOfWork(self.game_database, immediate=True) as uow:
            uow.attach_database(self.player_database, "player_data")
            uow.execute("CREATE TABLE IF NOT EXISTS natal_reawaken_operations(operation_id TEXT PRIMARY KEY,user_id TEXT NOT NULL,scripture_id INTEGER NOT NULL,scripture_cost INTEGER NOT NULL,max_slots INTEGER NOT NULL,choice_seed INTEGER NOT NULL,form INTEGER NOT NULL,name TEXT NOT NULL,effect_type INTEGER NOT NULL,base_value REAL NOT NULL,scripture_change INTEGER NOT NULL)")
            old = uow.query_one("SELECT user_id,scripture_id,scripture_cost,form,name,effect_type,base_value,scripture_change FROM natal_reawaken_operations WHERE operation_id=?", (operation_id,))
            if old is not None:
                if str(old["user_id"]) != user_id or int(old["scripture_id"]) != scripture_id or int(old["scripture_cost"]) != scripture_cost:
                    return self._result("state_changed")
                return self._result("duplicate", old["form"], old["name"], old["effect_type"], old["base_value"], old["scripture_change"])
            columns = {str(row["name"]) for row in uow.query_all("PRAGMA player_data.table_info(natal_treasure)")}
            required = {"form", "name", "level", "exp", "max_exp", "fate_revive_count", "immortal_revive_count", "invincible_gain_count", "nirvana_revive_count", "soul_return_revive_count", "charge_status", "soul_summon_count", "enlightenment_count"}
            for slot in range(1, max_slots + 1):
                required.update((f"effect{slot}_type", f"effect{slot}_base_value", f"effect{slot}_level"))
            if not required.issubset(columns):
                return self._result("treasure_missing")
            fields = ["form"] + [f"effect{slot}_level" for slot in range(1, max_slots + 1)]
            treasure = uow.query_one("SELECT " + ",".join(fields) + " FROM player_data.natal_treasure WHERE user_id=?", (user_id,))
            if treasure is None or int(treasure["form"] or 0) == 0:
                return self._result("treasure_missing")
            refund = sum(max(0, int(treasure[f"effect{slot}_level"] or 0) - 1) for slot in range(1, max_slots + 1))
            scripture_change = refund - scripture_cost
            item = uow.query_one("SELECT goods_num FROM back WHERE user_id=? AND goods_id=?", (user_id, scripture_id))
            current_quantity = int(item["goods_num"] or 0) if item else 0
            if scripture_change < 0 and current_quantity < -scripture_change:
                return self._result("item_insufficient", scripture_change=scripture_change)
            if scripture_change > 0 and current_quantity + scripture_change > max_goods_num:
                return self._result("inventory_full", scripture_change=scripture_change)
            rng = random.Random(choice_seed)
            form = rng.randint(1, 4)
            effect_type = rng.choice(sorted(configs))
            name = rng.choice(names[effect_type])
            minimum, maximum = configs[effect_type]
            base_value = minimum if effect_type in fixed else round(rng.uniform(minimum, maximum), 3)
            if scripture_change < 0:
                if uow.execute("UPDATE back SET goods_num=goods_num-? WHERE user_id=? AND goods_id=? AND goods_num>=?", (-scripture_change, user_id, scripture_id, -scripture_change)).rowcount != 1:
                    return self._result("state_changed")
            elif scripture_change > 0:
                if item is None:
                    uow.execute("INSERT INTO back(user_id,goods_id,goods_name,goods_type,goods_num,bind_num) VALUES(?,?,?,?,?,?)", (user_id, scripture_id, scripture_name, scripture_type, scripture_change, 0))
                elif uow.execute("UPDATE back SET goods_num=goods_num+? WHERE user_id=? AND goods_id=? AND goods_num+?<=?", (scripture_change, user_id, scripture_id, scripture_change, max_goods_num)).rowcount != 1:
                    return self._result("state_changed")
            assignments = [("form", form), ("name", name), ("level", 0), ("exp", 0), ("max_exp", 100), ("fate_revive_count", 0), ("immortal_revive_count", 0), ("invincible_gain_count", 0), ("nirvana_revive_count", 0), ("soul_return_revive_count", 0), ("charge_status", 0), ("soul_summon_count", "{}"), ("enlightenment_count", "{}")] 
            for slot in range(1, max_slots + 1):
                assignments.extend(((f"effect{slot}_type", effect_type if slot == 1 else 0), (f"effect{slot}_base_value", base_value if slot == 1 else 0.0), (f"effect{slot}_level", 1 if slot == 1 else 0)))
            updated = uow.execute("UPDATE player_data.natal_treasure SET " + ",".join(f'"{field}"=?' for field, _ in assignments) + " WHERE user_id=? AND form=?", tuple(value for _, value in assignments) + (user_id, int(treasure["form"])))
            if updated.rowcount != 1:
                return self._result("state_changed")
            uow.execute("INSERT INTO natal_reawaken_operations VALUES(?,?,?,?,?,?,?,?,?,?,?)", (operation_id, user_id, scripture_id, scripture_cost, max_slots, choice_seed, form, name, effect_type, base_value, scripture_change))
            return self._result("reawakened", form, name, effect_type, base_value, scripture_change)
