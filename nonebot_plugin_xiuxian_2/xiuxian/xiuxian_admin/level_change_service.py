from __future__ import annotations

import json
from contextlib import closing
from dataclasses import dataclass
from pathlib import Path
from threading import RLock

from ..xiuxian_utils import db_backend


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


class AdminLevelChangeService:
    """Replace a player's realm and derived combat state in one transaction."""

    def __init__(self, database: str | Path, lock: RLock | None = None) -> None:
        self._database = Path(database)
        self._lock = lock or RLock()

    @staticmethod
    def _ensure_schema(conn) -> None:
        conn.execute(
            "CREATE TABLE IF NOT EXISTS admin_level_change_operations("
            "operation_id TEXT PRIMARY KEY,payload TEXT NOT NULL,result_json TEXT NOT NULL,"
            "created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP)"
        )

    @staticmethod
    def _snapshot(row) -> tuple:
        return (
            str(row[0] or ""),
            int(row[1] or 0),
            int(row[2] or 0),
            int(row[3] or 0),
            int(row[4] or 0),
            int(row[5] or 0),
            str(row[6] or ""),
            int(row[7] or 0),
        )

    def change(
        self,
        operation_id,
        operator_id,
        user_id,
        expected_snapshot,
        new_level,
        new_exp,
        level_spend,
        root_rate,
    ) -> AdminLevelChangeResult:
        operation_id = str(operation_id).strip()
        operator_id = str(operator_id).strip()
        user_id = str(user_id).strip()
        new_level = str(new_level).strip()
        expected = tuple(expected_snapshot)
        new_exp = int(new_exp)
        level_spend = float(level_spend)
        root_rate = float(root_rate)
        if not operation_id or not operator_id or not user_id or not new_level:
            raise ValueError("operation, operator, user and level are required")
        if len(expected) != 8 or new_exp < 0 or level_spend <= 0 or root_rate <= 0:
            raise ValueError("invalid realm change snapshot or configuration")
        expected = (
            str(expected[0] or ""), int(expected[1] or 0), int(expected[2] or 0),
            int(expected[3] or 0), int(expected[4] or 0), int(expected[5] or 0),
            str(expected[6] or ""), int(expected[7] or 0),
        )
        payload = json.dumps(
            [operator_id, user_id, new_level, new_exp, level_spend, root_rate],
            ensure_ascii=True,
            separators=(",", ":"),
        )
        with self._lock, closing(db_backend.connect(self._database)) as conn:
            try:
                conn.execute("BEGIN IMMEDIATE")
                self._ensure_schema(conn)
                previous = conn.execute(
                    "SELECT payload,result_json FROM admin_level_change_operations WHERE operation_id=%s",
                    (operation_id,),
                ).fetchone()
                if previous is not None:
                    conn.rollback()
                    if str(previous[0]) != payload:
                        return AdminLevelChangeResult("operation_conflict")
                    return AdminLevelChangeResult("duplicate", **json.loads(str(previous[1])))

                row = conn.execute(
                    "SELECT level,COALESCE(exp,0),COALESCE(hp,0),COALESCE(mp,0),"
                    "COALESCE(atk,0),COALESCE(power,0),COALESCE(root_type,''),"
                    "COALESCE(root_level,0) FROM user_xiuxian WHERE user_id=%s",
                    (user_id,),
                ).fetchone()
                if row is None:
                    conn.rollback()
                    return AdminLevelChangeResult("user_missing")
                if self._snapshot(row) != expected:
                    conn.rollback()
                    return AdminLevelChangeResult("state_changed")

                changed = conn.execute(
                    "UPDATE user_xiuxian SET level=%s,exp=%s,hp=%s/2,mp=%s,atk=%s/10,"
                    "power=ROUND(%s*%s*%s,0) WHERE user_id=%s AND level=%s AND COALESCE(exp,0)=%s "
                    "AND COALESCE(hp,0)=%s AND COALESCE(mp,0)=%s AND COALESCE(atk,0)=%s "
                    "AND COALESCE(power,0)=%s AND COALESCE(root_type,'')=%s "
                    "AND COALESCE(root_level,0)=%s",
                    (
                        new_level, new_exp, new_exp, new_exp, new_exp, new_exp, root_rate,
                        level_spend, user_id, *expected,
                    ),
                )
                if changed.rowcount != 1:
                    conn.rollback()
                    return AdminLevelChangeResult("state_changed")
                result_row = conn.execute(
                    "SELECT level,exp,hp,mp,atk,power FROM user_xiuxian WHERE user_id=%s",
                    (user_id,),
                ).fetchone()
                result_data = {
                    "level": str(result_row[0]), "exp": int(result_row[1]),
                    "hp": int(result_row[2]), "mp": int(result_row[3]),
                    "atk": int(result_row[4]), "power": int(result_row[5]),
                }
                conn.execute(
                    "INSERT INTO admin_level_change_operations(operation_id,payload,result_json) "
                    "VALUES(%s,%s,%s)",
                    (operation_id, payload, json.dumps(result_data, ensure_ascii=True, separators=(",", ":"))),
                )
                conn.commit()
                return AdminLevelChangeResult("applied", **result_data)
            except Exception:
                conn.rollback()
                raise


__all__ = ["AdminLevelChangeResult", "AdminLevelChangeService"]
