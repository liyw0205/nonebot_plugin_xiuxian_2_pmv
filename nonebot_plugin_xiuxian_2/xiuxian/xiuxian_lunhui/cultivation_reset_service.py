from __future__ import annotations

import json
from contextlib import closing
from dataclasses import dataclass
from pathlib import Path
from threading import RLock

from ..xiuxian_utils import db_backend


@dataclass(frozen=True)
class CultivationResetResult:
    status: str
    reset_exp: int = 0

    @property
    def succeeded(self) -> bool:
        return self.status in {"applied", "duplicate"}


class CultivationResetService:
    def __init__(self, database: str | Path, lock: RLock | None = None):
        self._database = Path(database)
        self._lock = lock or RLock()

    def reset(self, operation_id: str, user_id: str, expected_level: str, expected_exp: int):
        operation_id = str(operation_id).strip()
        user_id = str(user_id)
        expected_level = str(expected_level)
        expected_exp = int(expected_exp)
        if not operation_id or expected_exp < 0:
            raise ValueError("invalid cultivation reset operation")
        payload = json.dumps(
            [user_id, expected_level, expected_exp],
            ensure_ascii=False,
            separators=(",", ":"),
        )

        with self._lock, closing(db_backend.connect(self._database)) as conn:
            try:
                conn.execute("BEGIN IMMEDIATE")
                conn.execute(
                    "CREATE TABLE IF NOT EXISTS cultivation_reset_operations ("
                    "operation_id TEXT PRIMARY KEY,payload TEXT NOT NULL,reset_exp INTEGER NOT NULL,"
                    "created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP)"
                )
                previous = conn.execute(
                    "SELECT payload,reset_exp FROM cultivation_reset_operations WHERE operation_id=%s",
                    (operation_id,),
                ).fetchone()
                if previous is not None:
                    conn.rollback()
                    if str(previous[0]) != payload:
                        return CultivationResetResult("operation_conflict")
                    return CultivationResetResult("duplicate", int(previous[1]))

                row = conn.execute(
                    "SELECT level,exp FROM user_xiuxian WHERE user_id=%s",
                    (user_id,),
                ).fetchone()
                if row is None:
                    conn.rollback()
                    return CultivationResetResult("user_missing")
                if str(row[0]) != expected_level or int(row[1]) != expected_exp:
                    conn.rollback()
                    return CultivationResetResult("state_changed")
                if expected_level not in {"感气境初期", "感气境中期", "感气境圆满"}:
                    conn.rollback()
                    return CultivationResetResult("level_rejected")

                changed = conn.execute(
                    "UPDATE user_xiuxian SET level='江湖好手',level_up_rate=0,exp=100,"
                    "power=0,hp=50,mp=100,atk=10 WHERE user_id=%s AND level=%s AND exp=%s",
                    (user_id, expected_level, expected_exp),
                )
                if changed.rowcount != 1:
                    conn.rollback()
                    return CultivationResetResult("state_changed")
                conn.execute(
                    "INSERT INTO cultivation_reset_operations(operation_id,payload,reset_exp) VALUES(%s,%s,%s)",
                    (operation_id, payload, expected_exp),
                )
                conn.commit()
                return CultivationResetResult("applied", expected_exp)
            except Exception:
                conn.rollback()
                raise


__all__ = ["CultivationResetResult", "CultivationResetService"]
