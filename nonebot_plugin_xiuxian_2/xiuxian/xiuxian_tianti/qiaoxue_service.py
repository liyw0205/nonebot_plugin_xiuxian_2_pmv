from __future__ import annotations

import json
from contextlib import closing
from dataclasses import dataclass
from pathlib import Path
from threading import RLock

from ..xiuxian_utils import db_backend
from .tianti_data import TiantiDataManager, get_qiaoxue_pool, get_tianti_level_data


@dataclass(frozen=True)
class QiaoxueResult:
    status: str
    user_id: str
    qiaoxue: dict
    hp_cost: int
    new_hp: int
    opened_count: int
    unlock_limit: int

    @property
    def succeeded(self) -> bool:
        return self.status in {"opened", "duplicate"}


class QiaoxueService:
    """Open one random qiaoxue in an idempotent player database transaction."""

    def __init__(self, player_database: str | Path, lock: RLock | None = None) -> None:
        self._player_database = Path(player_database)
        self._lock = lock or RLock()
        self._manager = TiantiDataManager()

    @staticmethod
    def _ensure_schema(conn, fields) -> None:
        conn.execute(
            "CREATE TABLE IF NOT EXISTS tianti_qiaoxue_operations ("
            "operation_id TEXT PRIMARY KEY, user_id TEXT NOT NULL, roll INTEGER NOT NULL, "
            "qiaoxue_json TEXT NOT NULL, hp_cost INTEGER NOT NULL, new_hp INTEGER NOT NULL, "
            "opened_count INTEGER NOT NULL, unlock_limit INTEGER NOT NULL, "
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
    def _result(status, user_id, qiaoxue=None, hp_cost=0, new_hp=0,
                opened_count=0, unlock_limit=0):
        return QiaoxueResult(
            status, user_id, dict(qiaoxue or {}), int(hp_cost), int(new_hp),
            int(opened_count), int(unlock_limit),
        )

    def get_result(self, operation_id: str) -> QiaoxueResult | None:
        operation_id = str(operation_id).strip()
        if not operation_id:
            return None
        with self._lock, closing(db_backend.connect(self._player_database)) as conn:
            self._ensure_schema(conn, tuple(self._manager._default().keys()))
            previous = conn.execute(
                "SELECT user_id, qiaoxue_json, hp_cost, new_hp, opened_count, unlock_limit "
                "FROM tianti_qiaoxue_operations WHERE operation_id=%s",
                (operation_id,),
            ).fetchone()
            if previous is None:
                return None
            return QiaoxueResult(
                "duplicate", str(previous[0]), json.loads(previous[1]),
                int(previous[2]), int(previous[3]), int(previous[4]), int(previous[5]),
            )

    def open(self, operation_id, user_id, roll) -> QiaoxueResult:
        operation_id = str(operation_id).strip()
        user_id = str(user_id)
        roll = int(roll)
        if not operation_id or roll < 0:
            raise ValueError("operation_id is required and roll must be non-negative")

        fields = tuple(self._manager._default().keys())
        with self._lock, closing(db_backend.connect(self._player_database)) as conn:
            try:
                conn.execute("BEGIN IMMEDIATE")
                self._ensure_schema(conn, fields)
                previous = conn.execute(
                    "SELECT user_id, roll, qiaoxue_json, hp_cost, new_hp, opened_count, unlock_limit "
                    "FROM tianti_qiaoxue_operations WHERE operation_id=%s", (operation_id,),
                ).fetchone()
                if previous is not None:
                    conn.rollback()
                    if str(previous[0]) != user_id:
                        return self._result("state_changed", user_id)
                    return self._result(
                        "duplicate", user_id, json.loads(previous[2]), *previous[3:]
                    )

                row = conn.execute(
                    "SELECT " + ", ".join(db_backend.quote_ident(field) for field in fields)
                    + " FROM tianti_info WHERE user_id=%s", (user_id,),
                ).fetchone()
                data = self._manager._clean_user_data(dict(zip(fields, row)) if row else {})
                opened = list(data.get("opened_qiaoxue", []))
                opened_count = len(opened)
                unlock_limit = min(int(get_tianti_level_data(data["tianti_level"])["rank"]) * 3, 108)
                if opened_count >= unlock_limit:
                    conn.rollback()
                    return self._result(
                        "limit_reached", user_id, opened_count=opened_count,
                        unlock_limit=unlock_limit,
                    )

                opened_names = set(opened)
                candidates = [item for item in get_qiaoxue_pool() if item["name"] not in opened_names]
                if not candidates:
                    conn.rollback()
                    return self._result(
                        "limit_reached", user_id, opened_count=opened_count,
                        unlock_limit=unlock_limit,
                    )
                old_hp = int(data["tianti_hp"])
                hp_cost = max(1, int(old_hp * 0.1))
                if old_hp < hp_cost:
                    conn.rollback()
                    return self._result(
                        "hp_insufficient", user_id, opened_count=opened_count,
                        unlock_limit=unlock_limit,
                    )

                chosen = dict(candidates[roll % len(candidates)])
                detail = list(data.get("opened_qiaoxue_detail", []))
                detail.append({
                    "name": chosen["name"], "group": chosen["group"],
                    "effect_type": chosen["effect_type"],
                    "effect_value": float(chosen["effect_value"]),
                })
                new_hp = old_hp - hp_cost
                data["tianti_hp"] = new_hp
                data["opened_qiaoxue"] = opened + [chosen["name"]]
                data["opened_qiaoxue_detail"] = detail
                if not isinstance(data.get("qiaoxue_stage_opened"), dict):
                    data["qiaoxue_stage_opened"] = {}

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
                new_count = opened_count + 1
                conn.execute(
                    "INSERT INTO tianti_qiaoxue_operations "
                    "(operation_id, user_id, roll, qiaoxue_json, hp_cost, new_hp, "
                    "opened_count, unlock_limit) VALUES (%s, %s, %s, %s, %s, %s, %s, %s)",
                    (operation_id, user_id, roll, json.dumps(chosen, ensure_ascii=False),
                     hp_cost, new_hp, new_count, unlock_limit),
                )
                conn.commit()
                return self._result(
                    "opened", user_id, chosen, hp_cost, new_hp, new_count, unlock_limit
                )
            except Exception:
                conn.rollback()
                raise


__all__ = ["QiaoxueResult", "QiaoxueService"]
