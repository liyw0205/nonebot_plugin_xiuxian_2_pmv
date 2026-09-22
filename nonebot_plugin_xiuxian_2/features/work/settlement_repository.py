from __future__ import annotations

import json
from pathlib import Path
from typing import Mapping

from ...infrastructure.database import DatabaseUnitOfWork


class WorkSettlementResult:
    def __init__(self, status: str, exp: int = 0, item_awarded: bool = False, result: dict | None = None) -> None:
        self.status = status
        self.exp = exp
        self.item_awarded = item_awarded
        self.result = result or {}


class WorkSettlementSqlRepository:
    def __init__(self, database: str | Path) -> None:
        self.database = str(database)

    def settle(self, operation_id: str, user_id: str, expected_work: Mapping[str, object], exp_gain: int, item: Mapping[str, object] | None, max_exp: int, max_goods_num: int = 0, **kwargs) -> WorkSettlementResult:
        operation_id, user_id = str(operation_id).strip(), str(user_id)
        exp_gain, max_exp = int(exp_gain), int(max_exp)
        payload = json.dumps([user_id], ensure_ascii=False, separators=(",", ":"))
        with DatabaseUnitOfWork(self.database, immediate=True) as uow:
            uow.execute("CREATE TABLE IF NOT EXISTS work_settlement_operations(operation_id TEXT PRIMARY KEY,payload TEXT NOT NULL,exp INTEGER NOT NULL,item_awarded INTEGER NOT NULL,result_json TEXT,created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP)")
            previous = uow.query_one("SELECT payload,exp,item_awarded,result_json FROM work_settlement_operations WHERE operation_id=?", (operation_id,))
            if previous is not None:
                if str(previous["payload"]) != payload:
                    return WorkSettlementResult("state_changed")
                return WorkSettlementResult("duplicate", int(previous["exp"]), bool(previous["item_awarded"]), json.loads(previous["result_json"] or "{}"))
            work = uow.query_one("SELECT type,create_time,scheduled_time FROM user_cd WHERE user_id=?", (user_id,))
            if work is None or int(work["type"] or 0) != 2 or str(work["create_time"]) != str(expected_work.get("create_time")) or str(work["scheduled_time"]) != str(expected_work.get("scheduled_time")):
                return WorkSettlementResult("state_changed")
            user = uow.query_one("SELECT COALESCE(exp,0) AS exp FROM user_xiuxian WHERE user_id=?", (user_id,))
            if user is None:
                return WorkSettlementResult("user_missing")
            current_exp = int(user["exp"])
            applied_exp = min(max_exp, current_exp + exp_gain)
            item_awarded = False
            result = {"exp": applied_exp}
            if item:
                item_id, quantity = int(item["goods_id"]), int(item["quantity"])
                existing = uow.query_one("SELECT goods_num FROM back WHERE user_id=? AND goods_id=?", (user_id, item_id))
                if existing is None:
                    uow.execute("INSERT INTO back(user_id,goods_id,goods_name,goods_type,goods_num,bind_num) VALUES(?,?,?,?,?,0)", (user_id, item_id, str(item["goods_name"]), str(item["goods_type"]), quantity))
                else:
                    uow.execute("UPDATE back SET goods_num=goods_num+? WHERE user_id=? AND goods_id=?", (quantity, user_id, item_id))
                item_awarded = True
            uow.execute("UPDATE user_xiuxian SET exp=? WHERE user_id=?", (applied_exp, user_id))
            uow.execute("UPDATE user_cd SET type=0 WHERE user_id=?", (user_id,))
            uow.execute("INSERT INTO work_settlement_operations(operation_id,payload,exp,item_awarded,result_json) VALUES(?,?,?,?,?)", (operation_id, payload, applied_exp, int(item_awarded), json.dumps(result, ensure_ascii=False)))
            return WorkSettlementResult("applied", applied_exp, item_awarded, result)


__all__ = ["WorkSettlementSqlRepository", "WorkSettlementResult"]
