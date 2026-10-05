from __future__ import annotations

import json
from datetime import datetime
from pathlib import Path
from typing import Any

from ...infrastructure.database import DatabaseUnitOfWork

ROOM_TABLE = "entertainment_game_rooms"
GAME_TYPES = frozenset({"gomoku", "half_ten", "minesweeper"})
MAX_ROOM_ROWS_PER_TYPE = 4096
MAX_ROOM_STATE_BYTES = 8 * 1024 * 1024
MAX_ROOM_RESTORE_BYTES = 128 * 1024 * 1024
_IDENTITY_FIELD = {"gomoku": "room_id", "half_ten": "room_id", "minesweeper": "game_id"}


class EntertainmentRoomSqlRepository:
    def __init__(self, database: str | Path) -> None:
        self.database = Path(database)
        self._schema_verified = False

    @staticmethod
    def _game_type(game_type: str) -> str:
        value = str(game_type)
        if value not in GAME_TYPES:
            raise ValueError("unsupported entertainment game type")
        return value

    @staticmethod
    def _schema_ready(uow: DatabaseUnitOfWork) -> bool:
        tables = {
            str(row["name"])
            for row in uow.query_all("SELECT name FROM sqlite_master WHERE type='table'")
        }
        if ROOM_TABLE not in tables:
            return False
        columns = {
            str(row["name"])
            for row in uow.query_all(f"PRAGMA table_info({ROOM_TABLE})")
        }
        return {"game_type", "room_id", "status", "state_json", "updated_at"} <= columns

    def _require_schema(self, uow: DatabaseUnitOfWork) -> None:
        if self._schema_verified:
            return
        if not self._schema_ready(uow):
            raise RuntimeError("legacy.entertainment.002 schema_missing: entertainment_game_rooms")
        self._schema_verified = True

    def _verify_schema(self) -> None:
        if self._schema_verified:
            return
        self._require_database_file()
        with DatabaseUnitOfWork(self.database, read_only=True, timeout=0) as uow:
            self._require_schema(uow)

    def _require_database_file(self) -> None:
        if not self.database.is_file():
            raise RuntimeError("legacy.entertainment.002 schema_missing: game database")

    @staticmethod
    def _encode(game_type: str, room_id: str, state: dict[str, Any]) -> tuple[str, str]:
        if not isinstance(state, dict):
            raise ValueError("entertainment room state must be an object")
        identity_field = _IDENTITY_FIELD[game_type]
        state_id = str(state.get(identity_field, ""))
        if not room_id or state_id != room_id:
            raise ValueError("entertainment room identity does not match its state")
        status = state.get("status")
        if not isinstance(status, str) or not status:
            raise ValueError("entertainment room status must be a non-empty string")
        payload = json.dumps(state, ensure_ascii=False, sort_keys=True, separators=(",", ":"))
        if len(payload.encode("utf-8")) > MAX_ROOM_STATE_BYTES:
            raise ValueError("entertainment room state exceeds storage limit")
        return status, payload

    @staticmethod
    def _decode(row: dict[str, Any]) -> dict[str, Any]:
        try:
            state = json.loads(str(row["state_json"]))
        except (TypeError, ValueError, RecursionError) as exc:
            raise RuntimeError("entertainment room state is corrupt") from exc
        if not isinstance(state, dict):
            raise RuntimeError("entertainment room state is not an object")
        game_type = str(row["game_type"])
        if game_type not in GAME_TYPES or str(state.get(_IDENTITY_FIELD[game_type], "")) != str(row["room_id"]):
            raise RuntimeError("entertainment room state identity is corrupt")
        return state

    def list_states(self, game_type: str) -> list[dict[str, Any]]:
        game_type = self._game_type(game_type)
        if not self.database.is_file():
            raise RuntimeError("legacy.entertainment.002 schema_missing: game database")
        with DatabaseUnitOfWork(self.database, read_only=True, timeout=0) as uow:
            self._require_schema(uow)
            bounds = uow.query_one(
                f"SELECT COUNT(*) AS row_count,"
                "COALESCE(SUM(length(CAST(state_json AS BLOB))),0) AS total_bytes "
                f"FROM {ROOM_TABLE} WHERE game_type=?",
                (game_type,),
            )
            if int(bounds["row_count"]) > MAX_ROOM_ROWS_PER_TYPE:
                raise RuntimeError("entertainment room restore exceeds row limit")
            if int(bounds["total_bytes"]) > MAX_ROOM_RESTORE_BYTES:
                raise RuntimeError("entertainment room restore exceeds byte limit")
            rows = uow.query_all(
                f"SELECT game_type,room_id,state_json FROM {ROOM_TABLE} "
                "WHERE game_type=? ORDER BY room_id",
                (game_type,),
            )
        return [self._decode(row) for row in rows]

    def save_state(self, game_type: str, room_id: str, state: dict[str, Any]) -> None:
        game_type = self._game_type(game_type)
        self._require_database_file()
        self._verify_schema()
        room_id = str(room_id)
        status, payload = self._encode(game_type, room_id, state)
        updated_at = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        with DatabaseUnitOfWork(self.database, immediate=True) as uow:
            uow.execute(
                f"INSERT INTO {ROOM_TABLE}(game_type,room_id,status,state_json,updated_at) "
                "VALUES(?,?,?,?,?) ON CONFLICT(game_type,room_id) DO UPDATE SET "
                "status=excluded.status,state_json=excluded.state_json,updated_at=excluded.updated_at",
                (game_type, room_id, status, payload, updated_at),
            )

    def delete_state(self, game_type: str, room_id: str) -> bool:
        game_type = self._game_type(game_type)
        self._require_database_file()
        self._verify_schema()
        with DatabaseUnitOfWork(self.database, immediate=True) as uow:
            cursor = uow.execute(
                f"DELETE FROM {ROOM_TABLE} WHERE game_type=? AND room_id=?",
                (game_type, str(room_id)),
            )
            return cursor.rowcount > 0


__all__ = [
    "EntertainmentRoomSqlRepository",
    "GAME_TYPES",
    "MAX_ROOM_RESTORE_BYTES",
    "MAX_ROOM_ROWS_PER_TYPE",
    "MAX_ROOM_STATE_BYTES",
]
