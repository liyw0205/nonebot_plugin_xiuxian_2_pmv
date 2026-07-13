from __future__ import annotations

import json
from contextlib import closing
from dataclasses import dataclass
from pathlib import Path
from threading import RLock

from ..xiuxian_utils import db_backend


@dataclass(frozen=True)
class AdminStoneAdjustmentResult:
    status: str
    previous_stone: int = 0
    final_stone: int = 0
    applied_delta: int = 0

    @property
    def succeeded(self) -> bool:
        return self.status in {"adjusted", "duplicate"}


class AdminStoneAdjustmentService:
    """Apply one administrator stone adjustment and economy audit atomically."""

    def __init__(self, database: str | Path, lock: RLock | None = None) -> None:
        self._database = Path(database)
        self._lock = lock or RLock()

    @staticmethod
    def _ensure_schema(conn) -> None:
        conn.execute(
            "CREATE TABLE IF NOT EXISTS admin_stone_adjustment_operations("
            "operation_id TEXT PRIMARY KEY,payload TEXT NOT NULL,previous_stone INTEGER NOT NULL,"
            "final_stone INTEGER NOT NULL,applied_delta INTEGER NOT NULL,"
            "created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP)"
        )
        conn.execute(
            "CREATE TABLE IF NOT EXISTS economy_log("
            "id INTEGER PRIMARY KEY AUTOINCREMENT,user_id TEXT,sect_id INTEGER,"
            "source TEXT NOT NULL,action TEXT NOT NULL,stone_delta INTEGER NOT NULL DEFAULT 0,"
            "exp_delta INTEGER NOT NULL DEFAULT 0,sect_contribution_delta INTEGER NOT NULL DEFAULT 0,"
            "sect_scale_delta INTEGER NOT NULL DEFAULT 0,sect_materials_delta INTEGER NOT NULL DEFAULT 0,"
            "item_delta TEXT NOT NULL DEFAULT '[]',detail TEXT NOT NULL DEFAULT '{}',"
            "trace_id TEXT,created_at TEXT NOT NULL)"
        )
        if not conn.column_exists("economy_log", "trace_id"):
            conn.execute("ALTER TABLE economy_log ADD COLUMN trace_id TEXT")

    def adjust(
        self,
        operation_id,
        operator_id,
        user_id,
        expected_stone,
        requested_delta,
        *,
        target_name="",
    ) -> AdminStoneAdjustmentResult:
        operation_id = str(operation_id).strip()
        operator_id = str(operator_id).strip()
        user_id = str(user_id).strip()
        expected_stone = int(expected_stone)
        requested_delta = int(requested_delta)
        target_name = str(target_name)
        if not operation_id or not operator_id or not user_id:
            raise ValueError("operation, operator and user are required")
        if expected_stone < 0 or requested_delta == 0:
            raise ValueError("valid stone snapshot and non-zero adjustment are required")

        payload = json.dumps(
            [operator_id, user_id, requested_delta],
            ensure_ascii=True,
            separators=(",", ":"),
        )
        with self._lock, closing(db_backend.connect(self._database)) as conn:
            try:
                conn.execute("BEGIN IMMEDIATE")
                self._ensure_schema(conn)
                previous = conn.execute(
                    "SELECT payload,previous_stone,final_stone,applied_delta "
                    "FROM admin_stone_adjustment_operations WHERE operation_id=%s",
                    (operation_id,),
                ).fetchone()
                if previous is not None:
                    conn.rollback()
                    if str(previous[0]) != payload:
                        return AdminStoneAdjustmentResult("operation_conflict")
                    return AdminStoneAdjustmentResult(
                        "duplicate", int(previous[1]), int(previous[2]), int(previous[3])
                    )

                row = conn.execute(
                    "SELECT COALESCE(stone,0) FROM user_xiuxian WHERE user_id=%s",
                    (user_id,),
                ).fetchone()
                if row is None:
                    conn.rollback()
                    return AdminStoneAdjustmentResult("user_missing")
                actual_stone = int(row[0])
                if actual_stone != expected_stone:
                    conn.rollback()
                    return AdminStoneAdjustmentResult(
                        "state_changed", actual_stone, actual_stone
                    )

                final_stone = max(0, expected_stone + requested_delta)
                applied_delta = final_stone - expected_stone
                changed = conn.execute(
                    "UPDATE user_xiuxian SET stone=%s WHERE user_id=%s AND COALESCE(stone,0)=%s",
                    (final_stone, user_id, expected_stone),
                )
                if changed.rowcount != 1:
                    conn.rollback()
                    return AdminStoneAdjustmentResult("state_changed")

                detail = json.dumps(
                    {
                        "operator_id": operator_id,
                        "target_name": target_name,
                        "requested_delta": requested_delta,
                        "previous_stone": expected_stone,
                        "final_stone": final_stone,
                    },
                    ensure_ascii=True,
                    sort_keys=True,
                    separators=(",", ":"),
                )
                conn.execute(
                    "INSERT INTO economy_log(user_id,source,action,stone_delta,item_delta,detail,trace_id,created_at) "
                    "VALUES(%s,'admin',%s,%s,'[]',%s,%s,CURRENT_TIMESTAMP)",
                    (
                        user_id,
                        "admin_stone_add" if applied_delta > 0 else "admin_stone_cost",
                        applied_delta,
                        detail,
                        operation_id,
                    ),
                )
                conn.execute(
                    "INSERT INTO admin_stone_adjustment_operations("
                    "operation_id,payload,previous_stone,final_stone,applied_delta) VALUES(%s,%s,%s,%s,%s)",
                    (operation_id, payload, expected_stone, final_stone, applied_delta),
                )
                conn.commit()
                return AdminStoneAdjustmentResult(
                    "adjusted", expected_stone, final_stone, applied_delta
                )
            except Exception:
                conn.rollback()
                raise
