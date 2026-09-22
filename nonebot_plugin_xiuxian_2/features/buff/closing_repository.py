from __future__ import annotations

import json
from pathlib import Path

from ...infrastructure.database import DatabaseUnitOfWork


class ClosingSettlementResult:
    def __init__(self, status: str, exp_gain: int = 0, stone_cost: int = 0, hp: int = 0, mp: int = 0, atk: int = 0, power: int = 0) -> None:
        self.status = status
        self.exp_gain, self.stone_cost, self.hp, self.mp, self.atk, self.power = exp_gain, stone_cost, hp, mp, atk, power


class ClosingSettlementSqlRepository:
    def __init__(self, database: str | Path) -> None:
        self.database = str(database)

    def settle(self, operation_id: str, user_id: str, expected_create_time: str, exp_gain: int, stone_cost: int, hp: int, mp: int, atk: int, power: int) -> ClosingSettlementResult:
        values = tuple(int(float(value)) for value in (exp_gain, stone_cost, hp, mp, atk, power))
        if not operation_id or min(values) < 0:
            raise ValueError("valid closing settlement values are required")
        payload = json.dumps([str(user_id), str(expected_create_time), *values], separators=(",", ":"))
        with DatabaseUnitOfWork(self.database, immediate=True) as uow:
            uow.execute("CREATE TABLE IF NOT EXISTS closing_settlement_operations(operation_id TEXT PRIMARY KEY,payload TEXT NOT NULL,result_json TEXT NOT NULL,created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP)")
            previous = uow.query_one("SELECT payload,result_json FROM closing_settlement_operations WHERE operation_id=?", (operation_id,))
            if previous is not None:
                if str(previous["payload"]) != payload:
                    return ClosingSettlementResult("state_changed")
                return ClosingSettlementResult("duplicate", *json.loads(str(previous["result_json"])))
            user = uow.query_one("SELECT COALESCE(stone,0) AS stone FROM user_xiuxian WHERE user_id=?", (user_id,))
            cd = uow.query_one("SELECT type,create_time FROM user_cd WHERE user_id=?", (user_id,))
            if user is None or cd is None:
                return ClosingSettlementResult("user_missing")
            if int(cd["type"] or 0) != 1 or str(cd["create_time"]) != str(expected_create_time):
                return ClosingSettlementResult("state_changed")
            if int(user["stone"]) < values[1]:
                return ClosingSettlementResult("stone_insufficient")
            changed = uow.execute("UPDATE user_xiuxian SET exp=COALESCE(exp,0)+?,stone=COALESCE(stone,0)-?,hp=?,mp=?,atk=?,power=? WHERE user_id=? AND stone>=?", (values[0], values[1], values[2], values[3], values[4], values[5], user_id, values[1]))
            cleared = uow.execute("UPDATE user_cd SET type=0,create_time=0,scheduled_time=NULL WHERE user_id=? AND type=1 AND CAST(create_time AS TEXT)=?", (user_id, str(expected_create_time)))
            if changed.rowcount != 1 or cleared.rowcount != 1:
                return ClosingSettlementResult("state_changed")
            uow.execute("INSERT INTO closing_settlement_operations(operation_id,payload,result_json) VALUES(?,?,?)", (operation_id, payload, json.dumps(values, separators=(",", ":"))))
            return ClosingSettlementResult("applied", *values)


__all__ = ["ClosingSettlementSqlRepository", "ClosingSettlementResult"]
