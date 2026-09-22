from __future__ import annotations

import json
from pathlib import Path

from ...infrastructure.database import DatabaseUnitOfWork


class StoneTrainingResult:
    def __init__(self, status: str, exp_gain: int = 0, stone_cost: int = 0, power: int = 0) -> None:
        self.status, self.exp_gain, self.stone_cost, self.power = status, exp_gain, stone_cost, power


class StoneTrainingSqlRepository:
    def __init__(self, game_database: str | Path, player_database: str | Path) -> None:
        self.game_database, self.player_database = str(game_database), str(player_database)

    def settle(self, operation_id: str, user_id: str, *, requested_stone: int, expected_exp: int, expected_stone: int, exp_cap: int, power_multiplier: float) -> StoneTrainingResult:
        requested_stone, expected_exp, expected_stone, exp_cap = map(int, (requested_stone, expected_exp, expected_stone, exp_cap))
        if not operation_id or requested_stone <= 0 or min(expected_exp, expected_stone, exp_cap) < 0:
            raise ValueError("invalid stone training settlement arguments")
        possible_exp = requested_stone // 10
        exp_gain = min(possible_exp, max(0, exp_cap - expected_exp))
        stone_cost = exp_gain * 10 if possible_exp >= max(0, exp_cap - expected_exp) else requested_stone
        payload = json.dumps([user_id, requested_stone, exp_cap, float(power_multiplier)], separators=(",", ":"))
        with DatabaseUnitOfWork(self.game_database, immediate=True) as uow:
            uow.execute("CREATE TABLE IF NOT EXISTS stone_training_operations(operation_id TEXT PRIMARY KEY,payload TEXT NOT NULL,result_json TEXT NOT NULL,created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP)")
            previous = uow.query_one("SELECT payload,result_json FROM stone_training_operations WHERE operation_id=?", (operation_id,))
            if previous is not None:
                if str(previous["payload"]) != payload:
                    return StoneTrainingResult("operation_conflict")
                return StoneTrainingResult("duplicate", **json.loads(str(previous["result_json"])))
            user = uow.query_one("SELECT COALESCE(exp,0) AS exp,COALESCE(stone,0) AS stone FROM user_xiuxian WHERE user_id=?", (user_id,))
            if user is None:
                return StoneTrainingResult("user_missing")
            if (int(user["exp"]), int(user["stone"])) != (expected_exp, expected_stone):
                return StoneTrainingResult("state_changed")
            if exp_gain <= 0:
                return StoneTrainingResult("exp_capped")
            if expected_stone < stone_cost:
                return StoneTrainingResult("stone_insufficient", exp_gain, stone_cost)
            changed = uow.execute("UPDATE user_xiuxian SET exp=exp+?,stone=stone-?,power=ROUND((exp+?)*?,0) WHERE user_id=? AND exp=? AND stone>=?", (exp_gain, stone_cost, exp_gain, float(power_multiplier), user_id, expected_exp, stone_cost))
            if changed.rowcount != 1:
                return StoneTrainingResult("state_changed")
            power_row = uow.query_one("SELECT power FROM user_xiuxian WHERE user_id=?", (user_id,))
            power = int(power_row["power"] or 0)
            saved = {"exp_gain": exp_gain, "stone_cost": stone_cost, "power": power}
            uow.execute("INSERT INTO stone_training_operations(operation_id,payload,result_json) VALUES(?,?,?)", (operation_id, payload, json.dumps(saved, separators=(",", ":"))))
            return StoneTrainingResult("applied", **saved)


__all__ = ["StoneTrainingSqlRepository", "StoneTrainingResult"]
