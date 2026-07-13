from __future__ import annotations

import json
from contextlib import closing
from dataclasses import dataclass
from pathlib import Path
from threading import RLock

from ..xiuxian_utils import db_backend


ROOT_CHANGES = {
    1: ("全属性灵根", "混沌灵根"),
    2: ("融合万物灵根", "融合灵根"),
    3: ("月灵根", "超灵根"),
    4: ("言灵灵根", "龙灵根"),
    5: ("金灵根", "天灵根"),
    6: ("轮回千次不灭，只为臻至巅峰", "轮回道果"),
    7: ("轮回万次不灭，只为超越巅峰", "真·轮回道果"),
    8: ("轮回无尽不灭，只为触及永恒之境", "永恒道果"),
}


@dataclass(frozen=True)
class AdminRootChangeResult:
    status: str
    root: str = ""
    root_type: str = ""
    power: int = 0

    @property
    def succeeded(self) -> bool:
        return self.status in {"applied", "duplicate"}


class AdminRootChangeService:
    """Change a player's root and recompute power atomically."""

    def __init__(self, database: str | Path, lock: RLock | None = None) -> None:
        self._database = Path(database)
        self._lock = lock or RLock()

    @staticmethod
    def _ensure_schema(conn) -> None:
        conn.execute(
            "CREATE TABLE IF NOT EXISTS admin_root_change_operations("
            "operation_id TEXT PRIMARY KEY,payload TEXT NOT NULL,result_json TEXT NOT NULL,"
            "created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP)"
        )

    @staticmethod
    def root_values(root_id: int, user_name: str) -> tuple[str, str]:
        root_id = int(root_id)
        if root_id == 9:
            return f"轮回命主·{user_name}", "命运道果"
        try:
            return ROOT_CHANGES[root_id]
        except KeyError as exc:
            raise ValueError("root_id must be between 1 and 9") from exc

    def change(
        self,
        operation_id,
        operator_id,
        user_id,
        expected_snapshot,
        root_id,
        level_spend,
        new_root_rate,
    ) -> AdminRootChangeResult:
        operation_id = str(operation_id).strip()
        operator_id = str(operator_id).strip()
        user_id = str(user_id).strip()
        root_id = int(root_id)
        expected = tuple(expected_snapshot)
        level_spend = float(level_spend)
        new_root_rate = float(new_root_rate)
        if not operation_id or not operator_id or not user_id:
            raise ValueError("operation, operator and user are required")
        if len(expected) != 7 or level_spend <= 0 or new_root_rate <= 0:
            raise ValueError("invalid root change snapshot or configuration")
        expected = (
            str(expected[0] or ""), str(expected[1] or ""), int(expected[2] or 0),
            str(expected[3] or ""), int(expected[4] or 0), int(expected[5] or 0),
            str(expected[6] or ""),
        )
        new_root, new_root_type = self.root_values(root_id, expected[6])
        payload = json.dumps(
            [operator_id, user_id, root_id, level_spend, new_root_rate],
            ensure_ascii=True,
            separators=(",", ":"),
        )
        with self._lock, closing(db_backend.connect(self._database)) as conn:
            try:
                conn.execute("BEGIN IMMEDIATE")
                self._ensure_schema(conn)
                previous = conn.execute(
                    "SELECT payload,result_json FROM admin_root_change_operations WHERE operation_id=%s",
                    (operation_id,),
                ).fetchone()
                if previous is not None:
                    conn.rollback()
                    if str(previous[0]) != payload:
                        return AdminRootChangeResult("operation_conflict")
                    return AdminRootChangeResult("duplicate", **json.loads(str(previous[1])))

                row = conn.execute(
                    "SELECT COALESCE(root,''),COALESCE(root_type,''),COALESCE(root_level,0),"
                    "COALESCE(level,''),COALESCE(exp,0),COALESCE(power,0),COALESCE(user_name,'') "
                    "FROM user_xiuxian WHERE user_id=%s",
                    (user_id,),
                ).fetchone()
                if row is None:
                    conn.rollback()
                    return AdminRootChangeResult("user_missing")
                actual = (
                    str(row[0]), str(row[1]), int(row[2]), str(row[3]), int(row[4]),
                    int(row[5]), str(row[6]),
                )
                if actual != expected:
                    conn.rollback()
                    return AdminRootChangeResult("state_changed")

                changed = conn.execute(
                    "UPDATE user_xiuxian SET root=%s,root_type=%s,power=ROUND(%s*%s*%s,0) "
                    "WHERE user_id=%s AND COALESCE(root,'')=%s AND COALESCE(root_type,'')=%s "
                    "AND COALESCE(root_level,0)=%s AND COALESCE(level,'')=%s "
                    "AND COALESCE(exp,0)=%s AND COALESCE(power,0)=%s AND COALESCE(user_name,'')=%s",
                    (new_root, new_root_type, expected[4], new_root_rate, level_spend, user_id, *expected),
                )
                if changed.rowcount != 1:
                    conn.rollback()
                    return AdminRootChangeResult("state_changed")
                power = int(conn.execute(
                    "SELECT power FROM user_xiuxian WHERE user_id=%s", (user_id,)
                ).fetchone()[0])
                result_data = {"root": new_root, "root_type": new_root_type, "power": power}
                conn.execute(
                    "INSERT INTO admin_root_change_operations(operation_id,payload,result_json) "
                    "VALUES(%s,%s,%s)",
                    (operation_id, payload, json.dumps(result_data, ensure_ascii=True, separators=(",", ":"))),
                )
                conn.commit()
                return AdminRootChangeResult("applied", **result_data)
            except Exception:
                conn.rollback()
                raise


__all__ = ["AdminRootChangeResult", "AdminRootChangeService", "ROOT_CHANGES"]
