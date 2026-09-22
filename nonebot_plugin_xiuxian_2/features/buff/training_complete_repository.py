from __future__ import annotations

import json
from pathlib import Path

from ...infrastructure.database import DatabaseUnitOfWork


class NormalTrainingCompleteResult:
    def __init__(self, status: str, kind: str = "", create_time: str = "", exp_gain: int = 0, stone_gain: int = 0, hp_gain: int = 0, mp_gain: int = 0) -> None:
        self.status, self.kind, self.create_time = status, kind, create_time
        self.exp_gain, self.stone_gain, self.hp_gain, self.mp_gain = exp_gain, stone_gain, hp_gain, mp_gain


class NormalTrainingCompleteSqlRepository:
    def __init__(self, game_database: str | Path, player_database: str | Path) -> None:
        self.game_database, self.player_database = str(game_database), str(player_database)

    def complete(self, operation_id: str, task_period: str) -> NormalTrainingCompleteResult:
        if not operation_id or not task_period:
            raise ValueError("operation and task period are required")
        with DatabaseUnitOfWork(self.game_database, immediate=True) as uow:
            uow.execute("ATTACH DATABASE ? AS player_data", (self.player_database,))
            uow.execute("CREATE TABLE IF NOT EXISTS normal_training_operations(operation_id TEXT PRIMARY KEY,user_id TEXT NOT NULL,payload TEXT NOT NULL,kind TEXT NOT NULL,create_time TEXT NOT NULL,scheduled_time TEXT NOT NULL,status TEXT NOT NULL,result_json TEXT,created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP)")
            row = uow.query_one("SELECT user_id,payload,kind,create_time,status,result_json FROM normal_training_operations WHERE operation_id=?", (operation_id,))
            if row is None:
                return NormalTrainingCompleteResult("operation_missing")
            if str(row["status"]) == "applied":
                saved = json.loads(str(row["result_json"] or "{}"))
                return NormalTrainingCompleteResult("duplicate", str(row["kind"]), str(row["create_time"]), **saved)
            user_id, values = str(row["user_id"]), json.loads(str(row["payload"]))
            expected_exp, expected_stone, reward, exp_cap = map(int, values[2:6])
            multiplier = float(values[6])
            user = uow.query_one("SELECT COALESCE(exp,0) AS exp,COALESCE(stone,0) AS stone,COALESCE(hp,0) AS hp,COALESCE(mp,0) AS mp FROM user_xiuxian WHERE user_id=?", (user_id,))
            cd = uow.query_one("SELECT type,create_time FROM user_cd WHERE user_id=?", (user_id,))
            if user is None or cd is None:
                return NormalTrainingCompleteResult("user_missing", str(row["kind"]), str(row["create_time"]))
            if (int(user["exp"]), int(user["stone"])) != (expected_exp, expected_stone) or int(cd["type"] or 0) != 5 or str(cd["create_time"]) != str(row["create_time"]):
                return NormalTrainingCompleteResult("state_changed", str(row["kind"]), str(row["create_time"]))
            kind = str(row["kind"])
            exp_gain = min(reward, max(0, exp_cap - expected_exp)) if kind == "cultivation" else 0
            stone_gain = reward if kind == "mining" else 0
            hp_gain = mp_gain = 0
            if kind == "cultivation":
                new_exp = expected_exp + exp_gain
                new_hp = min(new_exp // 2, int(user["hp"]) + expected_exp // 10)
                new_mp = min(new_exp, int(user["mp"]) + expected_exp // 20)
                hp_gain, mp_gain = max(0, new_hp - int(user["hp"])), max(0, new_mp - int(user["mp"]))
                changed = uow.execute("UPDATE user_xiuxian SET exp=?,hp=?,mp=?,atk=?,power=ROUND(?*?,0) WHERE user_id=? AND COALESCE(exp,0)=? AND COALESCE(stone,0)=?", (new_exp, new_hp, new_mp, expected_exp // 10, new_exp, multiplier, user_id, expected_exp, expected_stone))
            else:
                changed = uow.execute("UPDATE user_xiuxian SET stone=COALESCE(stone,0)+? WHERE user_id=? AND COALESCE(exp,0)=? AND COALESCE(stone,0)=?", (stone_gain, user_id, expected_exp, expected_stone))
            cleared = uow.execute("UPDATE user_cd SET type=0,create_time=0,scheduled_time=NULL WHERE user_id=? AND type=5 AND CAST(create_time AS TEXT)=?", (user_id, str(row["create_time"])))
            if changed.rowcount != 1 or cleared.rowcount != 1:
                return NormalTrainingCompleteResult("state_changed", kind, str(row["create_time"]))
            saved = {"exp_gain": exp_gain, "stone_gain": stone_gain, "hp_gain": hp_gain, "mp_gain": mp_gain}
            uow.execute("UPDATE normal_training_operations SET status='applied',result_json=? WHERE operation_id=? AND status='pending'", (json.dumps(saved, separators=(",", ":")), operation_id))
            return NormalTrainingCompleteResult("applied", kind, str(row["create_time"]), **saved)


__all__ = ["NormalTrainingCompleteSqlRepository", "NormalTrainingCompleteResult"]
