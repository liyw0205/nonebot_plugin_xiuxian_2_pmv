from __future__ import annotations

import json
from contextlib import closing
from dataclasses import dataclass
from pathlib import Path
from threading import RLock

from ..xiuxian_utils import db_backend


@dataclass(frozen=True)
class MapDongfuBuildResult:
    status: str
    stone: int

    @property
    def succeeded(self) -> bool:
        return self.status in {"applied", "duplicate"}


class MapDongfuBuildService:
    """Atomically spend stone and bind a dongfu to the current map node."""

    def __init__(self, game_database: str | Path, player_database: str | Path, lock: RLock | None = None) -> None:
        self._game_database = Path(game_database)
        self._player_database = Path(player_database)
        self._lock = lock or RLock()

    def build(self, operation_id, user_id, expected_stone, cost, expected_position, dongfu) -> MapDongfuBuildResult:
        operation_id, user_id = str(operation_id).strip(), str(user_id)
        expected_stone, cost = map(int, (expected_stone, cost))
        position = {key: str(value) for key, value in dict(expected_position).items()}
        dongfu = {key: str(value) for key, value in dict(dongfu).items()}
        if (
            not operation_id
            or min(expected_stone, cost) < 0
            or not {"realm", "heaven", "node_id"}.issubset(position)
            or dongfu.get("built") != "1"
        ):
            raise ValueError("valid operation, position and dongfu are required")
        payload = json.dumps([user_id, expected_stone, cost, position, dongfu], ensure_ascii=True, sort_keys=True)

        with self._lock, closing(db_backend.connect(self._game_database)) as conn:
            attached = False
            try:
                conn.execute("ATTACH DATABASE %s AS player_data", (str(self._player_database),))
                attached = True
                conn.execute("BEGIN IMMEDIATE")
                conn.execute(
                    "CREATE TABLE IF NOT EXISTS map_dongfu_build_operations ("
                    "operation_id TEXT PRIMARY KEY,payload TEXT NOT NULL,stone INTEGER NOT NULL,"
                    "created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP)"
                )
                old = conn.execute(
                    "SELECT payload,stone FROM map_dongfu_build_operations WHERE operation_id=%s", (operation_id,)
                ).fetchone()
                if old is not None:
                    conn.rollback()
                    if str(old[0]) != payload:
                        return MapDongfuBuildResult("state_changed", expected_stone)
                    return MapDongfuBuildResult("duplicate", int(old[1]))

                user = conn.execute("SELECT stone FROM user_xiuxian WHERE user_id=%s", (user_id,)).fetchone()
                if user is None:
                    conn.rollback()
                    return MapDongfuBuildResult("user_missing", expected_stone)
                stone = int(user[0] or 0)
                if stone != expected_stone:
                    conn.rollback()
                    return MapDongfuBuildResult("state_changed", stone)
                if stone < cost:
                    conn.rollback()
                    return MapDongfuBuildResult("stone_insufficient", stone)

                position_columns = self._columns(conn, "map_status")
                if not set(position).issubset(position_columns):
                    conn.rollback()
                    return MapDongfuBuildResult("state_changed", stone)
                current_position = conn.execute(
                    "SELECT " + ",".join(f'"{key}"' for key in position) + " FROM player_data.map_status WHERE user_id=%s",
                    (user_id,),
                ).fetchone()
                if current_position is None or tuple(str(value) for value in current_position) != tuple(position.values()):
                    conn.rollback()
                    return MapDongfuBuildResult("state_changed", stone)

                columns = self._columns(conn, "dongfu_status")
                for key in dongfu:
                    if key not in columns:
                        conn.execute(f'ALTER TABLE player_data.dongfu_status ADD COLUMN "{key}" TEXT DEFAULT NULL')
                existing = conn.execute("SELECT built FROM player_data.dongfu_status WHERE user_id=%s", (user_id,)).fetchone()
                if existing is not None and int(existing[0] or 0) == 1:
                    conn.rollback()
                    return MapDongfuBuildResult("already_built", stone)

                remaining = stone - cost
                conn.execute("UPDATE user_xiuxian SET stone=%s WHERE user_id=%s", (remaining, user_id))
                conn.execute(
                    "INSERT INTO player_data.dongfu_status (user_id," + ",".join(f'"{key}"' for key in dongfu) + ") "
                    "VALUES (" + ",".join(["%s"] * (len(dongfu) + 1)) + ") ON CONFLICT(user_id) DO UPDATE SET "
                    + ",".join(f'"{key}"=EXCLUDED."{key}"' for key in dongfu),
                    (user_id, *dongfu.values()),
                )
                conn.execute(
                    "INSERT INTO map_dongfu_build_operations (operation_id,payload,stone) VALUES (%s,%s,%s)",
                    (operation_id, payload, remaining),
                )
                conn.commit()
                return MapDongfuBuildResult("applied", remaining)
            except Exception:
                conn.rollback()
                raise
            finally:
                if attached:
                    conn.execute("DETACH DATABASE player_data")

    @staticmethod
    def _columns(conn, table: str) -> set[str]:
        return {str(row[1]) for row in conn.execute(f"PRAGMA player_data.table_info({table})").fetchall()}


__all__ = ["MapDongfuBuildResult", "MapDongfuBuildService"]
