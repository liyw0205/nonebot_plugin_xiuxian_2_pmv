from __future__ import annotations

import json
from dataclasses import dataclass
from pathlib import Path

from ...infrastructure.database import DatabaseUnitOfWork


ROOT_CHANGES = {1: ("全属性灵根", "混沌灵根"), 2: ("融合万物灵根", "融合灵根"), 3: ("月灵根", "超灵根"), 4: ("言灵灵根", "龙灵根"), 5: ("金灵根", "天灵根"), 6: ("轮回千次不灭，只为臻至巅峰", "轮回道果"), 7: ("轮回万次不灭，只为超越巅峰", "真·轮回道果"), 8: ("轮回无尽不灭，只为触及永恒之境", "永恒道果")}


@dataclass(frozen=True)
class AdminRootChangeResult:
    status: str
    root: str = ""
    root_type: str = ""
    power: int = 0

    @property
    def succeeded(self) -> bool:
        return self.status in {"applied", "duplicate"}


class AdminRootChangeSqlRepository:
    def __init__(self, database: str | Path) -> None:
        self.database = str(database)

    @staticmethod
    def root_values(root_id: int, user_name: str) -> tuple[str, str]:
        if int(root_id) == 9:
            return f"轮回命主·{user_name}", "命运道果"
        if int(root_id) not in ROOT_CHANGES:
            raise ValueError("root_id must be between 1 and 9")
        return ROOT_CHANGES[int(root_id)]

    def change(self, operation_id: str, operator_id: str, user_id: str, expected_snapshot, root_id: int, level_spend: float, new_root_rate: float, *, target_name: str = "") -> AdminRootChangeResult:
        operation_id, operator_id, user_id = str(operation_id).strip(), str(operator_id).strip(), str(user_id).strip()
        expected = tuple(expected_snapshot)
        if not operation_id or not operator_id or not user_id or len(expected) != 7 or float(level_spend) <= 0 or float(new_root_rate) <= 0:
            raise ValueError("invalid root change snapshot or configuration")
        expected = (str(expected[0] or ""), str(expected[1] or ""), int(expected[2] or 0), str(expected[3] or ""), int(expected[4] or 0), int(expected[5] or 0), str(expected[6] or ""))
        payload = json.dumps([operator_id, user_id, int(root_id), float(level_spend), float(new_root_rate)], ensure_ascii=True, separators=(",", ":"))
        with DatabaseUnitOfWork(self.database, immediate=True) as uow:
            uow.execute("CREATE TABLE IF NOT EXISTS admin_root_change_operations(operation_id TEXT PRIMARY KEY,payload TEXT NOT NULL,result_json TEXT NOT NULL,created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP)")
            previous = uow.query_one("SELECT payload,result_json FROM admin_root_change_operations WHERE operation_id=?", (operation_id,))
            if previous is not None:
                if str(previous["payload"]) != payload:
                    return AdminRootChangeResult("operation_conflict")
                return AdminRootChangeResult("duplicate", **json.loads(str(previous["result_json"])))
            row = uow.query_one("SELECT COALESCE(root,'') AS root,COALESCE(root_type,'') AS root_type,COALESCE(root_level,0) AS root_level,COALESCE(level,'') AS level,COALESCE(exp,0) AS exp,COALESCE(power,0) AS power,COALESCE(user_name,'') AS user_name FROM user_xiuxian WHERE user_id=?", (user_id,))
            if row is None:
                return AdminRootChangeResult("user_missing")
            actual = (str(row["root"]), str(row["root_type"]), int(row["root_level"]), str(row["level"]), int(row["exp"]), int(row["power"]), str(row["user_name"]))
            if actual != expected:
                return AdminRootChangeResult("state_changed")
            root, root_type = self.root_values(root_id, expected[6])
            power = round(expected[4] * float(new_root_rate) * float(level_spend))
            if uow.execute("UPDATE user_xiuxian SET root=?,root_type=?,power=? WHERE user_id=? AND root=? AND root_type=? AND root_level=? AND level=? AND exp=? AND power=? AND user_name=?", (root, root_type, power, user_id, *expected)).rowcount != 1:
                return AdminRootChangeResult("state_changed")
            data = {"root": root, "root_type": root_type, "power": power}
            uow.execute("INSERT INTO admin_root_change_operations(operation_id,payload,result_json) VALUES(?,?,?)", (operation_id, payload, json.dumps(data, ensure_ascii=True, separators=(",", ":"))))
            return AdminRootChangeResult("applied", **data)


__all__ = ["AdminRootChangeSqlRepository", "AdminRootChangeResult"]
