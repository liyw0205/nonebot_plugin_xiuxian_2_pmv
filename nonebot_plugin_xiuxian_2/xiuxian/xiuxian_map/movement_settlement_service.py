from __future__ import annotations

import json
from contextlib import closing
from dataclasses import dataclass
from pathlib import Path
from threading import RLock

from ..xiuxian_utils import db_backend


@dataclass(frozen=True)
class MapMovementResult:
    status: str
    stamina: int

    @property
    def succeeded(self) -> bool:
        return self.status in {"applied", "duplicate"}


class MapMovementSettlementService:
    """Atomically move a player, record the visit, and consume stamina."""

    def __init__(self, game_database: str | Path, player_database: str | Path, lock: RLock | None = None) -> None:
        self._game_database = Path(game_database)
        self._player_database = Path(player_database)
        self._lock = lock or RLock()

    def move(self, operation_id, user_id, expected_position, target_position, expected_stamina, cost):
        operation_id, user_id = str(operation_id).strip(), str(user_id)
        expected = self._position(expected_position)
        target = self._position(target_position)
        expected_stamina, cost = int(expected_stamina), int(cost)
        if not operation_id or expected_stamina < 0 or cost <= 0 or expected == target:
            raise ValueError("valid operation, distinct positions and positive cost are required")
        payload = json.dumps(
            [user_id, expected, target, expected_stamina, cost], ensure_ascii=True, sort_keys=True
        )

        with self._lock, closing(db_backend.connect(self._game_database)) as conn:
            attached = False
            try:
                conn.execute("ATTACH DATABASE %s AS player_data", (str(self._player_database),))
                attached = True
                conn.execute("BEGIN IMMEDIATE")
                conn.execute(
                    "CREATE TABLE IF NOT EXISTS map_movement_operations ("
                    "operation_id TEXT PRIMARY KEY,payload TEXT NOT NULL,stamina INTEGER NOT NULL,"
                    "created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP)"
                )
                old = conn.execute(
                    "SELECT payload,stamina FROM map_movement_operations WHERE operation_id=%s", (operation_id,)
                ).fetchone()
                if old is not None:
                    conn.rollback()
                    if str(old[0]) != payload:
                        return MapMovementResult("state_changed", expected_stamina)
                    return MapMovementResult("duplicate", int(old[1]))

                user = conn.execute(
                    "SELECT user_stamina FROM user_xiuxian WHERE user_id=%s", (user_id,)
                ).fetchone()
                if user is None:
                    conn.rollback()
                    return MapMovementResult("user_missing", expected_stamina)
                stamina = int(user[0] or 0)
                if stamina != expected_stamina:
                    conn.rollback()
                    return MapMovementResult("state_changed", stamina)
                if stamina < cost:
                    conn.rollback()
                    return MapMovementResult("stamina_insufficient", stamina)

                row = conn.execute(
                    "SELECT realm,heaven,node_id,visited_nodes FROM player_data.map_status WHERE user_id=%s",
                    (user_id,),
                ).fetchone()
                if row is None or tuple(str(value) for value in row[:3]) != tuple(expected.values()):
                    conn.rollback()
                    return MapMovementResult("state_changed", stamina)
                visited = self._visited(row[3])
                if target["node_id"] not in visited:
                    visited.append(target["node_id"])

                remaining = stamina - cost
                conn.execute(
                    "UPDATE player_data.map_status SET realm=%s,heaven=%s,node_id=%s,visited_nodes=%s "
                    "WHERE user_id=%s",
                    (*target.values(), json.dumps(visited, ensure_ascii=False), user_id),
                )
                conn.execute(
                    "UPDATE user_xiuxian SET user_stamina=%s WHERE user_id=%s", (remaining, user_id)
                )
                conn.execute(
                    "INSERT INTO map_movement_operations (operation_id,payload,stamina) VALUES (%s,%s,%s)",
                    (operation_id, payload, remaining),
                )
                conn.commit()
                return MapMovementResult("applied", remaining)
            except Exception:
                conn.rollback()
                raise
            finally:
                if attached:
                    conn.execute("DETACH DATABASE player_data")

    @staticmethod
    def _position(value):
        value = dict(value)
        if not {"realm", "heaven", "node_id"}.issubset(value):
            raise ValueError("position requires realm, heaven and node_id")
        return {key: str(value[key]) for key in ("realm", "heaven", "node_id")}

    @staticmethod
    def _visited(value):
        if isinstance(value, str):
            try:
                value = json.loads(value)
            except json.JSONDecodeError:
                value = []
        return [str(item) for item in value] if isinstance(value, list) else []


__all__ = ["MapMovementResult", "MapMovementSettlementService"]
