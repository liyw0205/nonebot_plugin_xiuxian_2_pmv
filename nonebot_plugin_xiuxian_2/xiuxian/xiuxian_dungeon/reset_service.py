from __future__ import annotations

import json
from collections.abc import Callable, Mapping
from contextlib import closing
from dataclasses import dataclass, field
from datetime import date, datetime
from pathlib import Path
from threading import RLock
from typing import Any

from ..xiuxian_utils import db_backend


_LOCKS_GUARD = RLock()
_DATABASE_LOCKS: dict[Path, RLock] = {}


def _database_lock(path: str | Path) -> RLock:
    resolved = Path(path).expanduser().resolve()
    with _LOCKS_GUARD:
        return _DATABASE_LOCKS.setdefault(resolved, RLock())


@dataclass(frozen=True)
class DungeonResetResult:
    status: str
    operation_id: str = ""
    business_date: str = ""
    generation: int = 0
    source: str = ""
    dungeon_snapshot: dict[str, Any] = field(default_factory=dict)
    reset_players: int = 0
    operation_status: str = ""

    @property
    def succeeded(self) -> bool:
        return self.status in {"applied", "duplicate"}


class DungeonResetService:
    """Publish one dungeon generation and reset every player in one transaction."""

    _AUTOMATIC_SOURCES = frozenset({"daily", "crossday"})
    _SOURCES = _AUTOMATIC_SOURCES | {"manual"}
    _GLOBAL_COLUMNS = {
        "user_id": "TEXT",
        "dungeon_id": "TEXT",
        "dungeon_name": "TEXT",
        "date": "TEXT",
        "total_layers": "INTEGER NOT NULL DEFAULT 0",
        "dungeon_type": "TEXT NOT NULL DEFAULT 'explore'",
        "description": "TEXT NOT NULL DEFAULT ''",
        "reset_generation": "INTEGER NOT NULL DEFAULT 0",
        "reset_operation_id": "TEXT NOT NULL DEFAULT ''",
    }
    _PLAYER_COLUMNS = {
        "user_id": "TEXT",
        "dungeon_id": "TEXT",
        "dungeon_name": "TEXT",
        "dungeon_status": "TEXT",
        "current_layer": "INTEGER",
        "total_layers": "INTEGER",
        "last_reset_date": "TEXT",
        "reset_generation": "INTEGER NOT NULL DEFAULT 0",
        "reset_operation_id": "TEXT NOT NULL DEFAULT ''",
    }
    _OPERATION_COLUMNS = {
        "operation_id": "TEXT",
        "business_date": "TEXT NOT NULL DEFAULT ''",
        "generation": "INTEGER NOT NULL DEFAULT 0",
        "source": "TEXT NOT NULL DEFAULT 'legacy'",
        "dungeon_snapshot": "TEXT NOT NULL DEFAULT '{}'",
        "result_json": "TEXT NOT NULL DEFAULT '{}'",
        "status": "TEXT NOT NULL DEFAULT 'completed'",
        "created_at": "TEXT NOT NULL DEFAULT ''",
        "updated_at": "TEXT NOT NULL DEFAULT ''",
    }

    def __init__(self, player_database: str | Path, lock: RLock | None = None) -> None:
        self._database = Path(player_database)
        self._lock = lock or _database_lock(self._database)

    @staticmethod
    def _normalize_date(value) -> str:
        if value is None:
            value = date.today()
        if isinstance(value, datetime):
            value = value.date()
        if isinstance(value, date):
            return value.isoformat()
        return date.fromisoformat(str(value).strip()).isoformat()

    @classmethod
    def automatic_operation_id(cls, business_date=None) -> str:
        """Return the shared durable ID used by daily and lazy cross-day reset."""

        return f"dungeon-reset:auto:{cls._normalize_date(business_date)}"

    @staticmethod
    def _json(value: Any) -> str:
        return json.dumps(
            value,
            ensure_ascii=True,
            sort_keys=True,
            separators=(",", ":"),
        )

    @classmethod
    def _normalize_snapshot(cls, value: Any) -> dict[str, Any]:
        if isinstance(value, Mapping):
            raw = dict(value)
        else:
            raw = {
                "dungeon_id": getattr(value, "id", None),
                "dungeon_name": getattr(value, "name", None),
                "total_layers": getattr(value, "total_layers", None),
                "dungeon_type": getattr(value, "type", None),
                "description": getattr(value, "description", None),
            }

        dungeon_id = str(raw.pop("dungeon_id", raw.pop("id", "")) or "").strip()
        dungeon_name = str(
            raw.pop("dungeon_name", raw.pop("name", "")) or ""
        ).strip()
        try:
            total_layers = int(raw.pop("total_layers", 0))
        except (TypeError, ValueError) as exc:
            raise ValueError("dungeon total_layers must be an integer") from exc
        if not dungeon_id or not dungeon_name or total_layers < 1:
            raise ValueError("valid dungeon id, name and total_layers are required")

        snapshot = {
            "dungeon_id": dungeon_id,
            "dungeon_name": dungeon_name,
            "total_layers": total_layers,
        }
        snapshot.update(raw)
        try:
            return json.loads(cls._json(snapshot))
        except (TypeError, ValueError) as exc:
            raise ValueError("dungeon snapshot must be JSON serializable") from exc

    @staticmethod
    def _ensure_columns(conn, table: str, columns: dict[str, str]) -> None:
        existing = {str(name).lower() for name in conn.column_names(table)}
        for name, definition in columns.items():
            if name.lower() in existing:
                continue
            conn.execute(
                f"ALTER TABLE {db_backend.quote_ident(table)} ADD COLUMN "
                f"{db_backend.quote_ident(name)} {definition}"
            )

    @classmethod
    def _ensure_schema(cls, conn) -> None:
        conn.execute(
            "CREATE TABLE IF NOT EXISTS dungeon_global_state("
            "user_id TEXT PRIMARY KEY,dungeon_id TEXT,dungeon_name TEXT,date TEXT)"
        )
        cls._ensure_columns(conn, "dungeon_global_state", cls._GLOBAL_COLUMNS)

        conn.execute(
            "CREATE TABLE IF NOT EXISTS player_dungeon_status("
            "user_id TEXT PRIMARY KEY,dungeon_id TEXT,dungeon_name TEXT,"
            "dungeon_status TEXT,current_layer INTEGER,total_layers INTEGER,"
            "last_reset_date TEXT)"
        )
        cls._ensure_columns(conn, "player_dungeon_status", cls._PLAYER_COLUMNS)

        conn.execute(
            "CREATE TABLE IF NOT EXISTS dungeon_reset_operations("
            "operation_id TEXT PRIMARY KEY,business_date TEXT NOT NULL,"
            "generation INTEGER NOT NULL,source TEXT NOT NULL,"
            "dungeon_snapshot TEXT NOT NULL,result_json TEXT NOT NULL,"
            "status TEXT NOT NULL,created_at TEXT NOT NULL,updated_at TEXT NOT NULL)"
        )
        cls._ensure_columns(
            conn, "dungeon_reset_operations", cls._OPERATION_COLUMNS
        )
        conn.execute(
            "CREATE UNIQUE INDEX IF NOT EXISTS dungeon_reset_operation_id_uq "
            "ON dungeon_reset_operations(operation_id)"
        )
        conn.execute(
            "CREATE INDEX IF NOT EXISTS dungeon_reset_business_date_idx "
            "ON dungeon_reset_operations(business_date,generation)"
        )

    @staticmethod
    def _decode_object(value) -> dict[str, Any]:
        try:
            decoded = json.loads(str(value or "{}"))
        except (TypeError, ValueError):
            return {}
        return decoded if isinstance(decoded, dict) else {}

    @classmethod
    def _stored_result(cls, row, status: str) -> DungeonResetResult:
        snapshot = cls._decode_object(row[4])
        result = cls._decode_object(row[5])
        return DungeonResetResult(
            status=status,
            operation_id=str(row[0] or ""),
            business_date=str(row[1] or ""),
            generation=int(row[2] or 0),
            source=str(row[3] or ""),
            dungeon_snapshot=snapshot,
            reset_players=int(result.get("reset_players", 0) or 0),
            operation_status=str(row[6] or ""),
        )

    @staticmethod
    def _same_request(row, business_date: str, source: str) -> bool:
        stored_source = str(row[3] or "")
        return str(row[1] or "") == business_date and (
            stored_source == source
            or {stored_source, source}.issubset(DungeonResetService._AUTOMATIC_SOURCES)
        )

    @staticmethod
    def _operation(conn, operation_id: str):
        return conn.execute(
            "SELECT operation_id,business_date,generation,source,dungeon_snapshot,"
            "result_json,status FROM dungeon_reset_operations WHERE operation_id=%s",
            (operation_id,),
        ).fetchone()

    @classmethod
    def _automatic_publication(cls, conn, business_date: str):
        placeholders = ",".join("%s" for _ in cls._AUTOMATIC_SOURCES)
        return conn.execute(
            "SELECT operation_id,business_date,generation,source,dungeon_snapshot,"
            "result_json,status FROM dungeon_reset_operations "
            f"WHERE business_date=%s AND source IN ({placeholders}) "
            "AND status='completed' ORDER BY generation LIMIT 1",
            (business_date, *sorted(cls._AUTOMATIC_SOURCES)),
        ).fetchone()

    def operation_result(self, operation_id) -> DungeonResetResult | None:
        """Return one published reset snapshot without creating a new generation."""

        operation_id = str(operation_id).strip()
        if not operation_id:
            return None
        with self._lock, closing(db_backend.connect(self._database)) as conn:
            try:
                conn.execute("BEGIN IMMEDIATE")
                self._ensure_schema(conn)
                row = self._operation(conn, operation_id)
                conn.commit()
            except Exception:
                conn.rollback()
                raise
        if row is None:
            return None
        return self._stored_result(row, "duplicate")

    @staticmethod
    def _upsert_global(
        conn,
        snapshot: dict[str, Any],
        business_date: str,
        generation: int,
        operation_id: str,
    ) -> None:
        values = (
            snapshot["dungeon_id"],
            snapshot["dungeon_name"],
            business_date,
            int(snapshot["total_layers"]),
            str(snapshot.get("dungeon_type", "explore")),
            str(snapshot.get("description", "")),
            int(generation),
            str(operation_id),
            "0",
        )
        updated = conn.execute(
            "UPDATE dungeon_global_state SET dungeon_id=%s,dungeon_name=%s,date=%s,"
            "total_layers=%s,dungeon_type=%s,description=%s,reset_generation=%s,"
            "reset_operation_id=%s "
            "WHERE user_id=%s",
            values,
        )
        if updated.rowcount == 0:
            conn.execute(
                "INSERT INTO dungeon_global_state("
                "dungeon_id,dungeon_name,date,total_layers,dungeon_type,description,"
                "reset_generation,reset_operation_id,user_id) "
                "VALUES(%s,%s,%s,%s,%s,%s,%s,%s,%s)",
                values,
            )

    def ensure_player_status(
        self,
        user_id,
        fallback_snapshot: Mapping[str, Any] | None = None,
    ) -> dict[str, Any]:
        """Return the current generation, initializing one player atomically."""

        user_id = str(user_id).strip()
        if not user_id:
            raise ValueError("user_id is required")
        fallback = (
            self._normalize_snapshot(fallback_snapshot)
            if fallback_snapshot is not None
            else None
        )
        with self._lock, closing(db_backend.connect(self._database)) as conn:
            try:
                conn.execute("BEGIN IMMEDIATE")
                self._ensure_schema(conn)
                global_row = conn.execute(
                    "SELECT dungeon_id,dungeon_name,date,total_layers,dungeon_type,"
                    "description,reset_generation,reset_operation_id "
                    "FROM dungeon_global_state WHERE user_id='0'"
                ).fetchone()
                if global_row is None:
                    raise RuntimeError("dungeon global state is missing")
                global_state = {
                    "dungeon_id": str(global_row[0] or ""),
                    "dungeon_name": str(global_row[1] or ""),
                    "date": str(global_row[2] or ""),
                    "total_layers": int(global_row[3] or 0),
                    "dungeon_type": str(global_row[4] or "explore"),
                    "description": str(global_row[5] or ""),
                    "reset_generation": int(global_row[6] or 0),
                    "reset_operation_id": str(global_row[7] or ""),
                }
                if global_state["total_layers"] < 1:
                    if fallback is None or fallback["dungeon_id"] != global_state["dungeon_id"]:
                        raise RuntimeError("dungeon global snapshot is incomplete")
                    global_state.update(
                        {
                            "dungeon_name": fallback["dungeon_name"],
                            "total_layers": fallback["total_layers"],
                            "dungeon_type": str(fallback.get("dungeon_type", "explore")),
                            "description": str(fallback.get("description", "")),
                        }
                    )
                    conn.execute(
                        "UPDATE dungeon_global_state SET dungeon_name=%s,total_layers=%s,"
                        "dungeon_type=%s,description=%s WHERE user_id='0'",
                        (
                            global_state["dungeon_name"],
                            global_state["total_layers"],
                            global_state["dungeon_type"],
                            global_state["description"],
                        ),
                    )

                columns = (
                    "dungeon_id",
                    "dungeon_name",
                    "dungeon_status",
                    "current_layer",
                    "total_layers",
                    "last_reset_date",
                    "reset_generation",
                    "reset_operation_id",
                )
                row = conn.execute(
                    "SELECT " + ",".join(columns)
                    + " FROM player_dungeon_status WHERE user_id=%s",
                    (user_id,),
                ).fetchone()
                needs_reset = row is None
                if row is not None:
                    needs_reset = (
                        str(row[0] or "") != global_state["dungeon_id"]
                        or str(row[5] or "") != global_state["date"]
                        or int(row[6] or 0) != global_state["reset_generation"]
                        or str(row[7] or "") != global_state["reset_operation_id"]
                    )
                values = (
                    global_state["dungeon_id"],
                    global_state["dungeon_name"],
                    "not_started",
                    0,
                    global_state["total_layers"],
                    global_state["date"],
                    global_state["reset_generation"],
                    global_state["reset_operation_id"],
                )
                if row is None:
                    conn.execute(
                        "INSERT INTO player_dungeon_status(user_id,"
                        + ",".join(columns)
                        + ") VALUES(%s,%s,%s,%s,%s,%s,%s,%s,%s)",
                        (user_id, *values),
                    )
                    row = values
                elif needs_reset:
                    conn.execute(
                        "UPDATE player_dungeon_status SET "
                        + ",".join(f"{column}=%s" for column in columns)
                        + " WHERE user_id=%s",
                        (*values, user_id),
                    )
                    row = values
                result = dict(zip(columns, row))
                result["current_layer"] = int(result["current_layer"] or 0)
                result["total_layers"] = int(result["total_layers"] or 0)
                result["reset_generation"] = int(result["reset_generation"] or 0)
                conn.commit()
                return result
            except Exception:
                conn.rollback()
                raise

    def reset(
        self,
        operation_id,
        business_date,
        source,
        dungeon_factory: Callable[[], Any],
        *,
        updated_at=None,
    ) -> DungeonResetResult:
        operation_id = str(operation_id).strip()
        business_date = self._normalize_date(business_date)
        source = str(source).strip().lower()
        updated_at = str(
            updated_at or datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        )
        if not operation_id or source not in self._SOURCES:
            raise ValueError("valid operation_id and reset source are required")
        if not callable(dungeon_factory):
            raise TypeError("dungeon_factory must be callable")

        with self._lock, closing(db_backend.connect(self._database)) as conn:
            try:
                conn.execute("BEGIN IMMEDIATE")
                self._ensure_schema(conn)

                previous = self._operation(conn, operation_id)
                if previous is not None:
                    if not self._same_request(previous, business_date, source):
                        conn.commit()
                        return self._stored_result(previous, "operation_conflict")
                    conn.commit()
                    return self._stored_result(previous, "duplicate")

                if source in self._AUTOMATIC_SOURCES:
                    published = self._automatic_publication(conn, business_date)
                    if published is not None:
                        conn.commit()
                        return self._stored_result(published, "duplicate")

                generation_row = conn.execute(
                    "SELECT COALESCE(MAX(generation),0) FROM dungeon_reset_operations "
                    "WHERE business_date=%s AND status='completed'",
                    (business_date,),
                ).fetchone()
                generation = int(generation_row[0] or 0) + 1
                snapshot = self._normalize_snapshot(dungeon_factory())

                self._upsert_global(
                    conn, snapshot, business_date, generation, operation_id
                )
                reset = conn.execute(
                    "UPDATE player_dungeon_status SET dungeon_id=%s,dungeon_name=%s,"
                    "dungeon_status='not_started',current_layer=0,total_layers=%s,"
                    "last_reset_date=%s,reset_generation=%s,reset_operation_id=%s",
                    (
                        snapshot["dungeon_id"],
                        snapshot["dungeon_name"],
                        snapshot["total_layers"],
                        business_date,
                        generation,
                        operation_id,
                    ),
                )
                reset_players = max(0, int(reset.rowcount))
                result_json = self._json(
                    {
                        "business_date": business_date,
                        "dungeon_snapshot": snapshot,
                        "generation": generation,
                        "operation_id": operation_id,
                        "reset_players": reset_players,
                        "source": source,
                        "status": "completed",
                    }
                )
                conn.execute(
                    "INSERT INTO dungeon_reset_operations("
                    "operation_id,business_date,generation,source,dungeon_snapshot,"
                    "result_json,status,created_at,updated_at) "
                    "VALUES(%s,%s,%s,%s,%s,%s,'completed',%s,%s)",
                    (
                        operation_id,
                        business_date,
                        generation,
                        source,
                        self._json(snapshot),
                        result_json,
                        updated_at,
                        updated_at,
                    ),
                )
                stored = self._operation(conn, operation_id)
                if stored is None:
                    raise db_backend.IntegrityError(
                        "dungeon reset operation was not persisted"
                    )
                result = self._stored_result(stored, "applied")
                conn.commit()
                return result
            except Exception:
                conn.rollback()
                raise


__all__ = ["DungeonResetResult", "DungeonResetService"]
