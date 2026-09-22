from __future__ import annotations

import json
from dataclasses import dataclass
from pathlib import Path

from ...infrastructure.database import DatabaseUnitOfWork


@dataclass(frozen=True)
class AdminLevelChangeResult:
    status: str
    level: str = ""
    exp: int = 0
    hp: int = 0
    mp: int = 0
    atk: int = 0
    power: int = 0

    @property
    def succeeded(self) -> bool:
        return self.status in {"applied", "duplicate"}


class AdminLevelChangeSqlRepository:
    def __init__(self, database: str | Path) -> None:
        self.database = str(database)

    @staticmethod
    def _snapshot(row) -> tuple:
        return (str(row["level"] or ""), int(row["exp"] or 0), int(row["hp"] or 0), int(row["mp"] or 0), int(row["atk"] or 0), int(row["power"] or 0), str(row["root_type"] or ""), int(row["root_level"] or 0))

    def change(self, operation_id: str, operator_id: str, user_id: str, expected_snapshot, new_level: str, new_exp: int, level_spend: float, root_rate: float, *, target_name: str = "") -> AdminLevelChangeResult:
        operation_id, operator_id, user_id, new_level = str(operation_id).strip(), str(operator_id).strip(), str(user_id).strip(), str(new_level).strip()
        expected = tuple(expected_snapshot)
        if not operation_id or not operator_id or not user_id or not new_level or len(expected) != 8 or int(new_exp) < 0 or float(level_spend) <= 0 or float(root_rate) <= 0:
            raise ValueError("invalid realm change snapshot or configuration")
        expected = (str(expected[0] or ""), *[int(value or 0) for value in expected[1:6]], str(expected[6] or ""), int(expected[7] or 0))
        payload = json.dumps([operator_id, user_id, new_level, int(new_exp), float(level_spend), float(root_rate)], ensure_ascii=True, separators=(",", ":"))
        with DatabaseUnitOfWork(self.database, immediate=True) as uow:
            uow.execute("CREATE TABLE IF NOT EXISTS admin_level_change_operations(operation_id TEXT PRIMARY KEY,payload TEXT NOT NULL,result_json TEXT NOT NULL,created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP)")
            previous = uow.query_one("SELECT payload,result_json FROM admin_level_change_operations WHERE operation_id=?", (operation_id,))
            if previous is not None:
                if str(previous["payload"]) != payload:
                    return AdminLevelChangeResult("operation_conflict")
                return AdminLevelChangeResult("duplicate", **json.loads(str(previous["result_json"])))
            row = uow.query_one("SELECT level,COALESCE(exp,0) AS exp,COALESCE(hp,0) AS hp,COALESCE(mp,0) AS mp,COALESCE(atk,0) AS atk,COALESCE(power,0) AS power,COALESCE(root_type,'') AS root_type,COALESCE(root_level,0) AS root_level FROM user_xiuxian WHERE user_id=?", (user_id,))
            if row is None:
                return AdminLevelChangeResult("user_missing")
            if self._snapshot(row) != expected:
                return AdminLevelChangeResult("state_changed")
            values = (new_level, int(new_exp), int(new_exp) // 2, int(new_exp), int(new_exp) // 10, round(int(new_exp) * float(root_rate) * float(level_spend)), user_id, *expected)
            if uow.execute("UPDATE user_xiuxian SET level=?,exp=?,hp=?,mp=?,atk=?,power=? WHERE user_id=? AND level=? AND COALESCE(exp,0)=? AND COALESCE(hp,0)=? AND COALESCE(mp,0)=? AND COALESCE(atk,0)=? AND COALESCE(power,0)=? AND COALESCE(root_type,'')=? AND COALESCE(root_level,0)=?", values).rowcount != 1:
                return AdminLevelChangeResult("state_changed")
            data = {"level": new_level, "exp": int(new_exp), "hp": int(new_exp) // 2, "mp": int(new_exp), "atk": int(new_exp) // 10, "power": round(int(new_exp) * float(root_rate) * float(level_spend))}
            uow.execute("INSERT INTO admin_level_change_operations(operation_id,payload,result_json) VALUES(?,?,?)", (operation_id, payload, json.dumps(data, ensure_ascii=True, separators=(",", ":"))))
            return AdminLevelChangeResult("applied", **data)


__all__ = ["AdminLevelChangeSqlRepository", "AdminLevelChangeResult"]
