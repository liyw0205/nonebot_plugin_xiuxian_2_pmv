from __future__ import annotations

import json
from contextlib import closing
from dataclasses import dataclass
from pathlib import Path
from threading import RLock

from ..xiuxian_utils import db_backend


@dataclass(frozen=True)
class AdminImpartStoneAdjustmentResult:
    status: str
    previous_stone: int = 0
    final_stone: int = 0
    applied_delta: int = 0

    @property
    def succeeded(self) -> bool:
        return self.status in {"adjusted", "duplicate"}


class AdminImpartStoneAdjustmentService:
    """Atomically adjust one player's impart stones and admin audit."""

    def __init__(
        self,
        game_database: str | Path,
        impart_database: str | Path,
        lock: RLock | None = None,
    ) -> None:
        self._game_database = Path(game_database)
        self._impart_database = Path(impart_database)
        self._lock = lock or RLock()

    @staticmethod
    def _ensure_schema(conn) -> None:
        conn.execute(
            "CREATE TABLE IF NOT EXISTS admin_impart_stone_operations("
            "operation_id TEXT PRIMARY KEY,payload TEXT NOT NULL,"
            "previous_stone INTEGER NOT NULL,final_stone INTEGER NOT NULL,"
            "applied_delta INTEGER NOT NULL,created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP)"
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
        conn.execute(
            "CREATE TABLE IF NOT EXISTS impart_data.xiuxian_impart("
            "user_id TEXT,stone_num INTEGER DEFAULT 0)"
        )
        columns = {
            str(row[1])
            for row in conn.execute(
                "PRAGMA impart_data.table_info(xiuxian_impart)"
            ).fetchall()
        }
        if "user_id" not in columns:
            conn.execute(
                "ALTER TABLE impart_data.xiuxian_impart ADD COLUMN user_id TEXT"
            )
        if "stone_num" not in columns:
            conn.execute(
                "ALTER TABLE impart_data.xiuxian_impart "
                "ADD COLUMN stone_num INTEGER DEFAULT 0"
            )

    def snapshot(self, user_id) -> int | None:
        user_id = str(user_id).strip()
        if not user_id:
            raise ValueError("user id is required")
        with self._lock, closing(db_backend.connect(self._impart_database)) as conn:
            if not conn.table_exists("xiuxian_impart"):
                return None
            if not {"user_id", "stone_num"}.issubset(
                set(conn.column_names("xiuxian_impart"))
            ):
                return None
            rows = conn.execute(
                "SELECT COALESCE(stone_num,0) FROM xiuxian_impart WHERE user_id=%s",
                (user_id,),
            ).fetchall()
            return int(rows[0][0]) if rows else None

    def adjust(
        self,
        operation_id,
        operator_id,
        user_id,
        expected_stone,
        requested_delta,
        *,
        target_name="",
    ) -> AdminImpartStoneAdjustmentResult:
        operation_id = str(operation_id).strip()
        operator_id = str(operator_id).strip()
        user_id = str(user_id).strip()
        expected_stone = (
            None if expected_stone is None else int(expected_stone)
        )
        requested_delta = int(requested_delta)
        target_name = str(target_name)
        if not operation_id or not operator_id or not user_id:
            raise ValueError("operation, operator and user are required")
        if expected_stone is not None and expected_stone < 0:
            raise ValueError("stone snapshot cannot be negative")
        if requested_delta == 0:
            raise ValueError("adjustment cannot be zero")

        payload = json.dumps(
            [operator_id, user_id, requested_delta],
            ensure_ascii=True,
            separators=(",", ":"),
        )
        with self._lock, closing(db_backend.connect(self._game_database)) as conn:
            conn.execute(
                "ATTACH DATABASE %s AS impart_data", (str(self._impart_database),)
            )
            try:
                conn.execute("BEGIN IMMEDIATE")
                self._ensure_schema(conn)
                previous = conn.execute(
                    "SELECT payload,previous_stone,final_stone,applied_delta "
                    "FROM admin_impart_stone_operations WHERE operation_id=%s",
                    (operation_id,),
                ).fetchone()
                if previous is not None:
                    conn.rollback()
                    if str(previous[0]) != payload:
                        return AdminImpartStoneAdjustmentResult(
                            "operation_conflict"
                        )
                    return AdminImpartStoneAdjustmentResult(
                        "duplicate",
                        int(previous[1]),
                        int(previous[2]),
                        int(previous[3]),
                    )

                if conn.execute(
                    "SELECT 1 FROM user_xiuxian WHERE user_id=%s", (user_id,)
                ).fetchone() is None:
                    conn.rollback()
                    return AdminImpartStoneAdjustmentResult("user_missing")
                rows = conn.execute(
                    "SELECT COALESCE(stone_num,0) "
                    "FROM impart_data.xiuxian_impart WHERE user_id=%s",
                    (user_id,),
                ).fetchall()
                if len(rows) > 1:
                    conn.rollback()
                    return AdminImpartStoneAdjustmentResult("invalid_state")
                actual_stone = int(rows[0][0]) if rows else None
                if actual_stone != expected_stone:
                    conn.rollback()
                    current = int(actual_stone or 0)
                    return AdminImpartStoneAdjustmentResult(
                        "state_changed", current, current
                    )

                previous_stone = int(actual_stone or 0)
                final_stone = max(0, previous_stone + requested_delta)
                applied_delta = final_stone - previous_stone
                if actual_stone is None:
                    conn.execute(
                        "INSERT INTO impart_data.xiuxian_impart(user_id,stone_num) "
                        "VALUES(%s,%s)",
                        (user_id, final_stone),
                    )
                else:
                    changed = conn.execute(
                        "UPDATE impart_data.xiuxian_impart SET stone_num=%s "
                        "WHERE user_id=%s AND COALESCE(stone_num,0)=%s",
                        (final_stone, user_id, previous_stone),
                    )
                    if changed.rowcount != 1:
                        conn.rollback()
                        return AdminImpartStoneAdjustmentResult("state_changed")

                item_delta = json.dumps(
                    [{
                        "id": "impart_stone",
                        "name": "思恋结晶",
                        "type": "传承货币",
                        "amount": applied_delta,
                    }],
                    ensure_ascii=False,
                    separators=(",", ":"),
                )
                detail = json.dumps(
                    {
                        "operator_id": operator_id,
                        "target_name": target_name,
                        "requested_delta": requested_delta,
                        "previous_stone": previous_stone,
                        "final_stone": final_stone,
                    },
                    ensure_ascii=False,
                    sort_keys=True,
                    separators=(",", ":"),
                )
                conn.execute(
                    "INSERT INTO economy_log("
                    "user_id,source,action,item_delta,detail,trace_id,created_at) "
                    "VALUES(%s,'admin',%s,%s,%s,%s,CURRENT_TIMESTAMP)",
                    (
                        user_id,
                        "admin_impart_stone_add"
                        if requested_delta > 0
                        else "admin_impart_stone_cost",
                        item_delta,
                        detail,
                        operation_id,
                    ),
                )
                conn.execute(
                    "INSERT INTO admin_impart_stone_operations("
                    "operation_id,payload,previous_stone,final_stone,applied_delta) "
                    "VALUES(%s,%s,%s,%s,%s)",
                    (
                        operation_id,
                        payload,
                        previous_stone,
                        final_stone,
                        applied_delta,
                    ),
                )
                conn.commit()
                return AdminImpartStoneAdjustmentResult(
                    "adjusted",
                    previous_stone,
                    final_stone,
                    applied_delta,
                )
            except Exception:
                conn.rollback()
                raise
            finally:
                conn.execute("DETACH DATABASE impart_data")


__all__ = [
    "AdminImpartStoneAdjustmentResult",
    "AdminImpartStoneAdjustmentService",
]
