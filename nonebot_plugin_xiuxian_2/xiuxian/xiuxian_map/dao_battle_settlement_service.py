from __future__ import annotations

import json
from contextlib import closing
from dataclasses import dataclass
from pathlib import Path
from threading import RLock

from ..xiuxian_utils import db_backend


@dataclass(frozen=True)
class MapDaoBattleResult:
    status: str

    @property
    def succeeded(self) -> bool:
        return self.status in {"applied", "duplicate"}


class MapDaoBattleSettlementService:
    """Atomically record both sides of a dao battle after position revalidation."""

    def __init__(self, player_database: str | Path, game_database: str | Path, lock: RLock | None = None) -> None:
        self._player_database = Path(player_database)
        self._game_database = Path(game_database)
        self._lock = lock or RLock()

    def settle(self, operation_id, challenger_id, target_id, expected_position, challenger_won):
        operation_id = str(operation_id).strip()
        challenger_id, target_id = str(challenger_id), str(target_id)
        position = self._position(expected_position)
        challenger_won = bool(challenger_won)
        if not operation_id or not challenger_id or challenger_id == target_id:
            raise ValueError("valid operation and distinct players are required")
        payload = json.dumps(
            [challenger_id, target_id, position, challenger_won], ensure_ascii=True, sort_keys=True
        )

        with self._lock, closing(db_backend.connect(self._player_database)) as conn:
            attached = False
            try:
                conn.execute("ATTACH DATABASE %s AS game_data", (str(self._game_database),))
                attached = True
                conn.execute("BEGIN IMMEDIATE")
                conn.execute(
                    "CREATE TABLE IF NOT EXISTS map_dao_battle_operations ("
                    "operation_id TEXT PRIMARY KEY,payload TEXT NOT NULL,"
                    "created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP)"
                )
                old = conn.execute(
                    "SELECT payload FROM map_dao_battle_operations WHERE operation_id=%s", (operation_id,)
                ).fetchone()
                if old is not None:
                    conn.rollback()
                    return MapDaoBattleResult("duplicate" if str(old[0]) == payload else "state_changed")

                placeholders = "%s,%s"
                users = conn.execute(
                    f"SELECT user_id FROM game_data.user_xiuxian WHERE user_id IN ({placeholders})",
                    (challenger_id, target_id),
                ).fetchall()
                if {str(row[0]) for row in users} != {challenger_id, target_id}:
                    conn.rollback()
                    return MapDaoBattleResult("user_missing")

                rows = conn.execute(
                    f"SELECT user_id,realm,heaven,node_id FROM map_status WHERE user_id IN ({placeholders})",
                    (challenger_id, target_id),
                ).fetchall()
                current = {str(row[0]): tuple(str(value) for value in row[1:]) for row in rows}
                expected = tuple(position.values())
                if current.get(challenger_id) != expected or current.get(target_id) != expected:
                    conn.rollback()
                    return MapDaoBattleResult("position_changed")

                conn.execute(
                    "CREATE TABLE IF NOT EXISTS dao_record ("
                    "user_id TEXT PRIMARY KEY,total INTEGER DEFAULT 0,win INTEGER DEFAULT 0,lose INTEGER DEFAULT 0)"
                )
                columns = set(conn.column_names("dao_record"))
                for column in ("total", "win", "lose"):
                    if column not in columns:
                        conn.execute(f'ALTER TABLE dao_record ADD COLUMN "{column}" INTEGER DEFAULT 0')

                self._increment(conn, challenger_id, challenger_won)
                self._increment(conn, target_id, not challenger_won)
                conn.execute(
                    "INSERT INTO map_dao_battle_operations (operation_id,payload) VALUES (%s,%s)",
                    (operation_id, payload),
                )
                conn.commit()
                return MapDaoBattleResult("applied")
            except Exception:
                conn.rollback()
                raise
            finally:
                if attached:
                    conn.execute("DETACH DATABASE game_data")

    @staticmethod
    def _increment(conn, user_id: str, won: bool) -> None:
        conn.execute(
            "INSERT INTO dao_record (user_id,total,win,lose) VALUES (%s,1,%s,%s) "
            "ON CONFLICT(user_id) DO UPDATE SET total=COALESCE(dao_record.total,0)+1,"
            "win=COALESCE(dao_record.win,0)+EXCLUDED.win,"
            "lose=COALESCE(dao_record.lose,0)+EXCLUDED.lose",
            (user_id, int(won), int(not won)),
        )

    @staticmethod
    def _position(value):
        value = dict(value)
        if not {"realm", "heaven", "node_id"}.issubset(value):
            raise ValueError("position requires realm, heaven and node_id")
        return {key: str(value[key]) for key in ("realm", "heaven", "node_id")}


__all__ = ["MapDaoBattleResult", "MapDaoBattleSettlementService"]
