from __future__ import annotations

import json
from dataclasses import dataclass
from pathlib import Path

from ...infrastructure.database import DatabaseUnitOfWork


@dataclass(frozen=True)
class AdminExpAdjustmentResult:
    status: str
    previous_exp: int = 0
    final_exp: int = 0
    applied_delta: int = 0

    @property
    def succeeded(self) -> bool:
        return self.status in {"adjusted", "duplicate"}


class AdminExpAdjustmentSqlRepository:
    def __init__(self, database: str | Path) -> None:
        self.database = str(database)

    def adjust(self, operation_id: str, operator_id: str, user_id: str, expected_exp: int, requested_delta: int, *, target_name: str = "") -> AdminExpAdjustmentResult:
        operation_id, operator_id, user_id = str(operation_id).strip(), str(operator_id).strip(), str(user_id).strip()
        expected_exp, requested_delta = int(expected_exp), int(requested_delta)
        if not operation_id or not operator_id or not user_id or expected_exp < 0 or requested_delta == 0:
            raise ValueError("valid experience adjustment request is required")
        payload = json.dumps([operator_id, user_id, requested_delta], ensure_ascii=True, separators=(",", ":"))
        with DatabaseUnitOfWork(self.database, immediate=True) as uow:
            uow.execute("CREATE TABLE IF NOT EXISTS admin_exp_adjustment_operations(operation_id TEXT PRIMARY KEY,payload TEXT NOT NULL,previous_exp INTEGER NOT NULL,final_exp INTEGER NOT NULL,applied_delta INTEGER NOT NULL,created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP)")
            uow.execute("CREATE TABLE IF NOT EXISTS economy_log(id INTEGER PRIMARY KEY AUTOINCREMENT,user_id TEXT,sect_id INTEGER,source TEXT NOT NULL,action TEXT NOT NULL,stone_delta INTEGER NOT NULL DEFAULT 0,exp_delta INTEGER NOT NULL DEFAULT 0,sect_contribution_delta INTEGER NOT NULL DEFAULT 0,sect_scale_delta INTEGER NOT NULL DEFAULT 0,sect_materials_delta INTEGER NOT NULL DEFAULT 0,item_delta TEXT NOT NULL DEFAULT '[]',detail TEXT NOT NULL DEFAULT '{}',trace_id TEXT,created_at TEXT NOT NULL)")
            previous = uow.query_one("SELECT payload,previous_exp,final_exp,applied_delta FROM admin_exp_adjustment_operations WHERE operation_id=?", (operation_id,))
            if previous is not None:
                if str(previous["payload"]) != payload:
                    return AdminExpAdjustmentResult("operation_conflict")
                return AdminExpAdjustmentResult("duplicate", int(previous["previous_exp"]), int(previous["final_exp"]), int(previous["applied_delta"]))
            row = uow.query_one("SELECT COALESCE(exp,0) AS exp FROM user_xiuxian WHERE user_id=?", (user_id,))
            if row is None:
                return AdminExpAdjustmentResult("user_missing")
            actual = int(row["exp"])
            if actual != expected_exp:
                return AdminExpAdjustmentResult("state_changed", actual, actual)
            final = max(0, expected_exp + requested_delta)
            applied = final - expected_exp
            if uow.execute("UPDATE user_xiuxian SET exp=? WHERE user_id=? AND COALESCE(exp,0)=?", (final, user_id, expected_exp)).rowcount != 1:
                return AdminExpAdjustmentResult("state_changed")
            detail = json.dumps({"operator_id": operator_id, "target_name": str(target_name), "requested_delta": requested_delta, "previous_exp": expected_exp, "final_exp": final}, ensure_ascii=True, sort_keys=True, separators=(",", ":"))
            uow.execute("INSERT INTO economy_log(user_id,source,action,exp_delta,item_delta,detail,trace_id,created_at) VALUES(?,'admin',?,?,'[]',?,?,CURRENT_TIMESTAMP)", (user_id, "admin_exp_add" if applied > 0 else "admin_exp_cost", applied, detail, operation_id))
            uow.execute("INSERT INTO admin_exp_adjustment_operations(operation_id,payload,previous_exp,final_exp,applied_delta) VALUES(?,?,?,?,?)", (operation_id, payload, expected_exp, final, applied))
            return AdminExpAdjustmentResult("adjusted", expected_exp, final, applied)


__all__ = ["AdminExpAdjustmentSqlRepository", "AdminExpAdjustmentResult"]
