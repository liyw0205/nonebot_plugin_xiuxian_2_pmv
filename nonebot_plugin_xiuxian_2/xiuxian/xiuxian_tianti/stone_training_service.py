from __future__ import annotations

import json
from contextlib import closing
from dataclasses import dataclass
from pathlib import Path
from threading import RLock

from ..xiuxian_utils import db_backend
from .tianti_data import TiantiDataManager
from .tianti_service import get_tianti_cap


@dataclass(frozen=True)
class StoneTrainingResult:
    status: str
    user_id: str
    requested_stone: int
    stone_cost: int
    hp_gain: int
    new_hp: int

    @property
    def succeeded(self) -> bool:
        return self.status in {"trained", "duplicate"}


class StoneTrainingService:
    """Exchange stones for tianti HP across attached SQLite databases."""

    def __init__(self, game_database: str | Path, player_database: str | Path,
                 lock: RLock | None = None) -> None:
        self._game_database = Path(game_database)
        self._player_database = Path(player_database)
        self._lock = lock or RLock()
        self._manager = TiantiDataManager()

    @staticmethod
    def _ensure_schema(conn, fields) -> None:
        conn.execute(
            "CREATE TABLE IF NOT EXISTS tianti_stone_training_operations ("
            "operation_id TEXT PRIMARY KEY, user_id TEXT NOT NULL, requested_stone INTEGER NOT NULL, "
            "stone_cost INTEGER NOT NULL, hp_gain INTEGER NOT NULL, new_hp INTEGER NOT NULL, "
            "created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP)"
        )
        conn.execute("CREATE TABLE IF NOT EXISTS player_data.tianti_info (user_id TEXT PRIMARY KEY)")
        columns = {
            str(row[1]) for row in conn.execute(
                "PRAGMA player_data.table_info(tianti_info)"
            ).fetchall()
        }
        for field in fields:
            if field not in columns:
                conn.execute(
                    f"ALTER TABLE player_data.tianti_info ADD COLUMN {db_backend.quote_ident(field)} TEXT"
                )

    def train(self, operation_id, user_id, requested_stone) -> StoneTrainingResult:
        operation_id = str(operation_id).strip()
        user_id = str(user_id)
        requested_stone = int(requested_stone)
        if not operation_id or requested_stone <= 0:
            raise ValueError("operation_id and requested_stone must be positive")

        def result(status, stone_cost=0, hp_gain=0, new_hp=0):
            return StoneTrainingResult(
                status, user_id, requested_stone, int(stone_cost), int(hp_gain), int(new_hp)
            )

        fields = tuple(self._manager._default().keys())
        with self._lock, closing(db_backend.connect(self._game_database)) as conn:
            conn.execute("ATTACH DATABASE %s AS player_data", (str(self._player_database),))
            try:
                conn.execute("BEGIN IMMEDIATE")
                self._ensure_schema(conn, fields)
                previous = conn.execute(
                    "SELECT user_id, requested_stone, stone_cost, hp_gain, new_hp "
                    "FROM tianti_stone_training_operations WHERE operation_id=%s",
                    (operation_id,),
                ).fetchone()
                if previous is not None:
                    conn.rollback()
                    if str(previous[0]) != user_id or int(previous[1]) != requested_stone:
                        return result("state_changed")
                    return result("duplicate", previous[2], previous[3], previous[4])

                user = conn.execute(
                    "SELECT COALESCE(stone, 0) FROM user_xiuxian WHERE user_id=%s", (user_id,)
                ).fetchone()
                if user is None:
                    conn.rollback()
                    return result("user_missing")
                if int(user[0]) < requested_stone:
                    conn.rollback()
                    return result("stone_insufficient")

                row = conn.execute(
                    "SELECT " + ", ".join(db_backend.quote_ident(field) for field in fields)
                    + " FROM player_data.tianti_info WHERE user_id=%s", (user_id,)
                ).fetchone()
                data = self._manager._clean_user_data(dict(zip(fields, row)) if row else {})
                old_hp = int(data["tianti_hp"])
                cap = get_tianti_cap(data)
                requested_gain = requested_stone // 10
                new_hp = min(cap, old_hp + requested_gain)
                hp_gain = max(0, new_hp - old_hp)
                stone_cost = hp_gain * 10
                if stone_cost <= 0:
                    conn.rollback()
                    return result("at_cap", new_hp=old_hp)

                charged = conn.execute(
                    "UPDATE user_xiuxian SET stone=stone-%s WHERE user_id=%s AND stone>=%s",
                    (stone_cost, user_id, stone_cost),
                )
                if charged.rowcount != 1:
                    conn.rollback()
                    return result("stone_changed")
                data["tianti_hp"] = new_hp
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
                    f"INSERT INTO player_data.tianti_info ({columns}) VALUES ({placeholders}) "
                    f"ON CONFLICT (user_id) DO UPDATE SET {updates}",
                    (user_id, *values),
                )
                conn.execute(
                    "INSERT INTO tianti_stone_training_operations "
                    "(operation_id, user_id, requested_stone, stone_cost, hp_gain, new_hp) "
                    "VALUES (%s, %s, %s, %s, %s, %s)",
                    (operation_id, user_id, requested_stone, stone_cost, hp_gain, new_hp),
                )
                conn.commit()
                return result("trained", stone_cost, hp_gain, new_hp)
            except Exception:
                conn.rollback()
                raise
            finally:
                conn.execute("DETACH DATABASE player_data")


__all__ = ["StoneTrainingResult", "StoneTrainingService"]
