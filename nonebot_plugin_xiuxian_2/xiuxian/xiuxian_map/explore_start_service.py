from __future__ import annotations

import json
from contextlib import closing
from dataclasses import dataclass
from pathlib import Path
from threading import RLock

from ..xiuxian_utils import db_backend


@dataclass(frozen=True)
class MapExploreStartResult:
    status: str
    stamina: int

    @property
    def succeeded(self) -> bool:
        return self.status in {"applied", "duplicate"}


class MapExploreStartService:
    """Atomically spend stamina and create a long-running exploration."""

    def __init__(self, game_database: str | Path, player_database: str | Path, lock: RLock | None = None) -> None:
        self._game_database = Path(game_database)
        self._player_database = Path(player_database)
        self._lock = lock or RLock()

    def start(
        self,
        operation_id,
        user_id,
        expected_stamina,
        stamina_cost,
        expected_position,
        expected_status,
        expected_daily,
        daily_limit,
        expected_cooldown,
        cooldown_until,
        new_status,
    ) -> MapExploreStartResult:
        operation_id, user_id = str(operation_id).strip(), str(user_id)
        expected_stamina, stamina_cost, daily_limit = map(int, (expected_stamina, stamina_cost, daily_limit))
        position = {key: str(value) for key, value in dict(expected_position).items()}
        status = {key: str(value) for key, value in dict(expected_status).items()}
        daily = {key: str(value) for key, value in dict(expected_daily).items()}
        expected_cooldown = "" if expected_cooldown is None else str(expected_cooldown)
        cooldown_until = str(cooldown_until)
        new_status = {key: str(value) for key, value in dict(new_status).items()}
        if (
            not operation_id
            or min(expected_stamina, stamina_cost, daily_limit) < 0
            or not {"realm", "heaven", "node_id"}.issubset(position)
            or status.get("running", "0") != "0"
            or not daily.get("date")
            or new_status.get("running") != "1"
        ):
            raise ValueError("valid operation and exploration snapshots are required")
        payload = json.dumps(
            [
                user_id,
                expected_stamina,
                stamina_cost,
                position,
                status,
                daily,
                daily_limit,
                expected_cooldown,
                cooldown_until,
                new_status,
            ],
            ensure_ascii=True,
            sort_keys=True,
        )

        with self._lock, closing(db_backend.connect(self._game_database)) as conn:
            attached = False
            try:
                conn.execute("ATTACH DATABASE %s AS player_data", (str(self._player_database),))
                attached = True
                conn.execute("BEGIN IMMEDIATE")
                conn.execute(
                    "CREATE TABLE IF NOT EXISTS map_explore_start_operations ("
                    "operation_id TEXT PRIMARY KEY,payload TEXT NOT NULL,stamina INTEGER NOT NULL,"
                    "created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP)"
                )
                old = conn.execute(
                    "SELECT payload,stamina FROM map_explore_start_operations WHERE operation_id=%s", (operation_id,)
                ).fetchone()
                if old is not None:
                    conn.rollback()
                    if str(old[0]) != payload:
                        return MapExploreStartResult("state_changed", expected_stamina)
                    return MapExploreStartResult("duplicate", int(old[1]))

                user = conn.execute("SELECT user_stamina FROM user_xiuxian WHERE user_id=%s", (user_id,)).fetchone()
                if user is None:
                    conn.rollback()
                    return MapExploreStartResult("user_missing", expected_stamina)
                stamina = int(user[0] or 0)
                if stamina != expected_stamina:
                    conn.rollback()
                    return MapExploreStartResult("state_changed", stamina)
                if stamina < stamina_cost:
                    conn.rollback()
                    return MapExploreStartResult("stamina_insufficient", stamina)

                if not self._matches_row(conn, "map_status", user_id, position):
                    conn.rollback()
                    return MapExploreStartResult("state_changed", stamina)
                if not self._matches_row(conn, "map_explore_status", user_id, status):
                    conn.rollback()
                    return MapExploreStartResult("already_running", stamina)
                if not self._matches_row(conn, "map_daily_limit", user_id, daily):
                    conn.rollback()
                    return MapExploreStartResult("state_changed", stamina)
                if int(daily.get("explore_count", 0)) >= daily_limit:
                    conn.rollback()
                    return MapExploreStartResult("limit_reached", stamina)

                cooldown_columns = self._columns(conn, "map_cooldown")
                if "explore_start_cd_until" not in cooldown_columns:
                    conn.execute('ALTER TABLE player_data.map_cooldown ADD COLUMN "explore_start_cd_until" TEXT DEFAULT NULL')
                cooldown_row = conn.execute(
                    'SELECT "explore_start_cd_until" FROM player_data.map_cooldown WHERE user_id=%s', (user_id,)
                ).fetchone()
                current_cooldown = "" if cooldown_row is None or cooldown_row[0] is None else str(cooldown_row[0])
                if current_cooldown != expected_cooldown:
                    conn.rollback()
                    return MapExploreStartResult("state_changed", stamina)

                explore_columns = self._columns(conn, "map_explore_status")
                if not set(new_status).issubset(explore_columns):
                    conn.rollback()
                    return MapExploreStartResult("state_changed", stamina)
                conn.execute("UPDATE user_xiuxian SET user_stamina=%s WHERE user_id=%s", (stamina - stamina_cost, user_id))
                conn.execute(
                    "UPDATE player_data.map_explore_status SET "
                    + ",".join(f'"{key}"=%s' for key in new_status)
                    + " WHERE user_id=%s",
                    (*new_status.values(), user_id),
                )
                conn.execute(
                    'INSERT INTO player_data.map_cooldown (user_id,"explore_start_cd_until") VALUES (%s,%s) '
                    'ON CONFLICT(user_id) DO UPDATE SET "explore_start_cd_until"=EXCLUDED."explore_start_cd_until"',
                    (user_id, cooldown_until),
                )
                remaining = stamina - stamina_cost
                conn.execute(
                    "INSERT INTO map_explore_start_operations (operation_id,payload,stamina) VALUES (%s,%s,%s)",
                    (operation_id, payload, remaining),
                )
                conn.commit()
                return MapExploreStartResult("applied", remaining)
            except Exception:
                conn.rollback()
                raise
            finally:
                if attached:
                    conn.execute("DETACH DATABASE player_data")

    @staticmethod
    def _columns(conn, table: str) -> set[str]:
        return {str(row[1]) for row in conn.execute(f"PRAGMA player_data.table_info({table})").fetchall()}

    @classmethod
    def _matches_row(cls, conn, table: str, user_id: str, expected: dict[str, str]) -> bool:
        columns = cls._columns(conn, table)
        if not expected or not set(expected).issubset(columns):
            return False
        row = conn.execute(
            "SELECT " + ",".join(f'"{key}"' for key in expected) + f" FROM player_data.{table} WHERE user_id=%s",
            (user_id,),
        ).fetchone()
        return row is not None and tuple(str(value) for value in row) == tuple(expected.values())


__all__ = ["MapExploreStartResult", "MapExploreStartService"]
