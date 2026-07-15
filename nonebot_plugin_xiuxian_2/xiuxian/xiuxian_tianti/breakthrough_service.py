from __future__ import annotations

import json
from contextlib import closing
from dataclasses import dataclass
from pathlib import Path
from threading import RLock

from ..xiuxian_utils import db_backend
from .tianti_data import (
    TiantiDataManager,
    get_next_tianti_level_name,
    get_tianti_level_data,
    get_tianti_level_index,
)


@dataclass(frozen=True)
class TiantiBreakthroughResult:
    status: str
    user_id: str
    old_level: str
    new_level: str
    hp_cost: int
    new_hp: int
    success: bool

    @property
    def succeeded(self) -> bool:
        return self.status in {"completed", "duplicate"}


class TiantiBreakthroughService:
    """Resolve one breakthrough attempt atomically and idempotently."""

    def __init__(self, player_database: str | Path, lock: RLock | None = None) -> None:
        self._player_database = Path(player_database)
        self._lock = lock or RLock()
        self._manager = TiantiDataManager()

    @staticmethod
    def _ensure_schema(conn, fields) -> None:
        conn.execute(
            "CREATE TABLE IF NOT EXISTS tianti_breakthrough_operations ("
            "operation_id TEXT PRIMARY KEY, user_id TEXT NOT NULL, cultivation_rank INTEGER NOT NULL, "
            "roll_success INTEGER NOT NULL, old_level TEXT NOT NULL, new_level TEXT NOT NULL, "
            "hp_cost INTEGER NOT NULL, new_hp INTEGER NOT NULL, success INTEGER NOT NULL, "
            "created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP)"
        )
        conn.execute("CREATE TABLE IF NOT EXISTS tianti_info (user_id TEXT PRIMARY KEY)")
        columns = {
            str(row[1]) for row in conn.execute("PRAGMA table_info(tianti_info)").fetchall()
        }
        for field in fields:
            if field not in columns:
                conn.execute(
                    f"ALTER TABLE tianti_info ADD COLUMN {db_backend.quote_ident(field)} TEXT"
                )

    @staticmethod
    def _result(status, user_id, row=None):
        if row is None:
            return TiantiBreakthroughResult(status, user_id, "", "", 0, 0, False)
        return TiantiBreakthroughResult(
            status, user_id, str(row[0]), str(row[1]), int(row[2]), int(row[3]), bool(row[4])
        )

    def get_result(self, operation_id: str) -> TiantiBreakthroughResult | None:
        operation_id = str(operation_id).strip()
        if not operation_id:
            return None
        with self._lock, closing(db_backend.connect(self._player_database)) as conn:
            self._ensure_schema(conn, tuple(self._manager._default().keys()))
            previous = conn.execute(
                "SELECT user_id, old_level, new_level, hp_cost, new_hp, success "
                "FROM tianti_breakthrough_operations WHERE operation_id=%s",
                (operation_id,),
            ).fetchone()
            if previous is None:
                return None
            return TiantiBreakthroughResult(
                "duplicate", str(previous[0]), str(previous[1]), str(previous[2]),
                int(previous[3]), int(previous[4]), bool(previous[5]),
            )

    def attempt(self, operation_id, user_id, *, cultivation_rank: int,
                roll_success: bool) -> TiantiBreakthroughResult:
        operation_id = str(operation_id).strip()
        user_id = str(user_id)
        cultivation_rank = int(cultivation_rank)
        roll_success = bool(roll_success)
        if not operation_id:
            raise ValueError("operation_id is required")

        fields = tuple(self._manager._default().keys())
        with self._lock, closing(db_backend.connect(self._player_database)) as conn:
            try:
                conn.execute("BEGIN IMMEDIATE")
                self._ensure_schema(conn, fields)
                previous = conn.execute(
                    "SELECT user_id, cultivation_rank, roll_success, old_level, new_level, "
                    "hp_cost, new_hp, success FROM tianti_breakthrough_operations "
                    "WHERE operation_id=%s", (operation_id,),
                ).fetchone()
                if previous is not None:
                    conn.rollback()
                    if (
                        str(previous[0]) != user_id
                        or int(previous[1]) != cultivation_rank
                    ):
                        return self._result("state_changed", user_id)
                    return self._result("duplicate", user_id, previous[3:])

                row = conn.execute(
                    "SELECT " + ", ".join(db_backend.quote_ident(field) for field in fields)
                    + " FROM tianti_info WHERE user_id=%s", (user_id,),
                ).fetchone()
                data = self._manager._clean_user_data(dict(zip(fields, row)) if row else {})
                old_level = str(data["tianti_level"])
                next_level = get_next_tianti_level_name(old_level)
                if not next_level:
                    conn.rollback()
                    return self._result("max_level", user_id)
                next_config = get_tianti_level_data(next_level)
                required_rank = get_tianti_level_index(
                    next_config["min_xx_level"], is_xiuxian=True
                )
                if cultivation_rank > required_rank:
                    conn.rollback()
                    return self._result("cultivation_insufficient", user_id)

                required_hp = int(next_config["need_hp"])
                old_hp = int(data["tianti_hp"])
                if old_hp < required_hp:
                    conn.rollback()
                    return self._result("hp_insufficient", user_id)

                hp_cost = max(1, int(old_hp * 0.05))
                new_hp = max(0, old_hp - hp_cost)
                new_level = next_level if roll_success else old_level
                data["tianti_hp"] = new_hp
                data["tianti_level"] = new_level
                values = [
                    json.dumps(data[field], ensure_ascii=False)
                    if isinstance(data[field], (list, dict)) else data[field]
                    for field in fields
                ]
                columns = ", ".join(["user_id", *(db_backend.quote_ident(field) for field in fields)])
                placeholders = ", ".join(["%s"] * (len(fields) + 1))
                updates = ", ".join(
                    f"{db_backend.quote_ident(field)}=EXCLUDED.{db_backend.quote_ident(field)}"
                    for field in fields
                )
                conn.execute(
                    f"INSERT INTO tianti_info ({columns}) VALUES ({placeholders}) "
                    f"ON CONFLICT (user_id) DO UPDATE SET {updates}", (user_id, *values),
                )
                conn.execute(
                    "INSERT INTO tianti_breakthrough_operations "
                    "(operation_id, user_id, cultivation_rank, roll_success, old_level, new_level, "
                    "hp_cost, new_hp, success) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)",
                    (operation_id, user_id, cultivation_rank, int(roll_success), old_level,
                     new_level, hp_cost, new_hp, int(roll_success)),
                )
                conn.commit()
                return self._result(
                    "completed", user_id,
                    (old_level, new_level, hp_cost, new_hp, int(roll_success)),
                )
            except Exception:
                conn.rollback()
                raise


__all__ = ["TiantiBreakthroughResult", "TiantiBreakthroughService"]
