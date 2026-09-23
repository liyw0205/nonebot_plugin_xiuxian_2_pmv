from __future__ import annotations

import json
from datetime import date, datetime
from pathlib import Path
from typing import Any, Callable, Mapping

from ...infrastructure.clock import SystemClock
from ...infrastructure.database import DatabaseUnitOfWork


class DungeonResetResult(dict):
    @property
    def status(self) -> str:
        return str(self["status"])

    @property
    def succeeded(self) -> bool:
        return self.status in {"applied", "duplicate"}

    def __getattr__(self, name: str) -> Any:
        try:
            return self[name]
        except KeyError as exc:
            raise AttributeError(name) from exc


class DungeonResetSqlRepository:
    AUTOMATIC_SOURCES = frozenset({"daily", "crossday"})
    SOURCES = AUTOMATIC_SOURCES | {"manual"}

    GLOBAL_COLUMNS = {
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
    PLAYER_COLUMNS = {
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
    OPERATION_COLUMNS = {
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

    def __init__(self, database: str | Path, *, clock: Any | None = None) -> None:
        self.database = str(database)
        self.clock = clock or SystemClock()

    @staticmethod
    def _json(value: Any) -> str:
        return json.dumps(value, ensure_ascii=True, sort_keys=True, separators=(",", ":"))

    @staticmethod
    def _date(value: Any) -> str:
        if isinstance(value, datetime):
            value = value.date()
        if isinstance(value, date):
            return value.isoformat()
        return date.fromisoformat(str(value).strip()).isoformat()

    @classmethod
    def automatic_operation_id(cls, business_date: Any = None) -> str:
        return f"dungeon-reset:auto:{cls._date(business_date or date.today())}"

    @classmethod
    def _snapshot(cls, value: Any) -> dict[str, Any]:
        raw = dict(value) if isinstance(value, Mapping) else {
            "dungeon_id": getattr(value, "id", ""),
            "dungeon_name": getattr(value, "name", ""),
            "total_layers": getattr(value, "total_layers", 0),
            "dungeon_type": getattr(value, "type", "explore"),
            "description": getattr(value, "description", ""),
        }
        snapshot = {
            "dungeon_id": str(raw.get("dungeon_id", raw.get("id", ""))).strip(),
            "dungeon_name": str(raw.get("dungeon_name", raw.get("name", ""))).strip(),
            "total_layers": int(raw.get("total_layers", 0) or 0),
            "dungeon_type": str(raw.get("dungeon_type", raw.get("type", "explore")) or "explore"),
            "description": str(raw.get("description", "") or ""),
        }
        if not snapshot["dungeon_id"] or not snapshot["dungeon_name"] or snapshot["total_layers"] < 1:
            raise ValueError("valid dungeon id, name and total_layers are required")
        return json.loads(cls._json({**raw, **snapshot}))

    @staticmethod
    def _columns(uow: DatabaseUnitOfWork, table: str) -> set[str]:
        return {str(row["name"]) for row in uow.query_all(f"PRAGMA table_info({table})")}

    @classmethod
    def _ensure_columns(cls, uow: DatabaseUnitOfWork, table: str, columns: Mapping[str, str]) -> None:
        existing = cls._columns(uow, table)
        for name, definition in columns.items():
            if name not in existing:
                uow.execute(f'ALTER TABLE "{table}" ADD COLUMN "{name}" {definition}')

    @classmethod
    def ensure_schema(cls, uow: DatabaseUnitOfWork) -> None:
        uow.execute("CREATE TABLE IF NOT EXISTS dungeon_global_state(user_id TEXT PRIMARY KEY,dungeon_id TEXT,dungeon_name TEXT,date TEXT)")
        cls._ensure_columns(uow, "dungeon_global_state", cls.GLOBAL_COLUMNS)
        uow.execute("CREATE TABLE IF NOT EXISTS player_dungeon_status(user_id TEXT PRIMARY KEY,dungeon_id TEXT,dungeon_name TEXT,dungeon_status TEXT,current_layer INTEGER,total_layers INTEGER,last_reset_date TEXT)")
        cls._ensure_columns(uow, "player_dungeon_status", cls.PLAYER_COLUMNS)
        uow.execute("CREATE TABLE IF NOT EXISTS dungeon_reset_operations(operation_id TEXT PRIMARY KEY,business_date TEXT NOT NULL,generation INTEGER NOT NULL,source TEXT NOT NULL,dungeon_snapshot TEXT NOT NULL,result_json TEXT NOT NULL,status TEXT NOT NULL,created_at TEXT NOT NULL,updated_at TEXT NOT NULL)")
        cls._ensure_columns(uow, "dungeon_reset_operations", cls.OPERATION_COLUMNS)
        uow.execute("CREATE UNIQUE INDEX IF NOT EXISTS dungeon_reset_operation_id_uq ON dungeon_reset_operations(operation_id)")
        uow.execute("CREATE INDEX IF NOT EXISTS dungeon_reset_business_date_idx ON dungeon_reset_operations(business_date,generation)")

    @staticmethod
    def _decode(value: Any) -> dict[str, Any]:
        try:
            result = json.loads(str(value or "{}"))
        except (TypeError, ValueError, json.JSONDecodeError):
            return {}
        return result if isinstance(result, dict) else {}

    @classmethod
    def _result(cls, row: Mapping[str, Any], status: str) -> DungeonResetResult:
        result = cls._decode(row.get("result_json"))
        return DungeonResetResult({
            "status": status,
            "operation_id": str(row.get("operation_id", "")),
            "business_date": str(row.get("business_date", "")),
            "generation": int(row.get("generation", 0) or 0),
            "source": str(row.get("source", "")),
            "dungeon_snapshot": cls._decode(row.get("dungeon_snapshot")),
            "reset_players": int(result.get("reset_players", 0) or 0),
            "operation_status": str(row.get("status", "")),
        })

    @classmethod
    def _operation(cls, uow: DatabaseUnitOfWork, operation_id: str) -> dict[str, Any] | None:
        return uow.query_one("SELECT operation_id,business_date,generation,source,dungeon_snapshot,result_json,status FROM dungeon_reset_operations WHERE operation_id=?", (operation_id,))

    @classmethod
    def _automatic(cls, uow: DatabaseUnitOfWork, business_date: str) -> dict[str, Any] | None:
        return uow.query_one("SELECT operation_id,business_date,generation,source,dungeon_snapshot,result_json,status FROM dungeon_reset_operations WHERE business_date=? AND source IN (?,?) AND status='completed' ORDER BY generation LIMIT 1", (business_date, "crossday", "daily"))

    def reset(self, operation_id: str, business_date: Any, source: str, dungeon_factory: Callable[[], Any]) -> dict[str, Any]:
        operation_id, business_date, source = str(operation_id).strip(), self._date(business_date), str(source).strip().lower()
        if not operation_id or source not in self.SOURCES:
            raise ValueError("valid operation_id and reset source are required")
        if not callable(dungeon_factory):
            raise TypeError("dungeon_factory must be callable")
        now = self.clock.now().isoformat() if hasattr(self.clock, "now") else str(self.clock())
        with DatabaseUnitOfWork(self.database, immediate=True) as uow:
            self.ensure_schema(uow)
            previous = self._operation(uow, operation_id)
            if previous is not None:
                same_date = str(previous["business_date"]) == business_date
                same_source = str(previous["source"]) == source or {str(previous["source"]), source}.issubset(self.AUTOMATIC_SOURCES)
                return self._result(previous, "duplicate" if same_date and same_source else "operation_conflict")
            if source in self.AUTOMATIC_SOURCES:
                published = self._automatic(uow, business_date)
                if published is not None:
                    return self._result(published, "duplicate")
            generation_row = uow.query_one("SELECT COALESCE(MAX(generation),0) AS generation FROM dungeon_reset_operations WHERE business_date=? AND status='completed'", (business_date,))
            generation = int(generation_row["generation"] if generation_row else 0) + 1
            snapshot = self._snapshot(dungeon_factory())
            values = (snapshot["dungeon_id"], snapshot["dungeon_name"], business_date, snapshot["total_layers"], snapshot["dungeon_type"], snapshot["description"], generation, operation_id)
            updated = uow.execute("UPDATE dungeon_global_state SET dungeon_id=?,dungeon_name=?,date=?,total_layers=?,dungeon_type=?,description=?,reset_generation=?,reset_operation_id=? WHERE user_id='0'", values)
            if updated.rowcount == 0:
                uow.execute("INSERT INTO dungeon_global_state(user_id,dungeon_id,dungeon_name,date,total_layers,dungeon_type,description,reset_generation,reset_operation_id) VALUES('0',?,?,?,?,?,?,?,?)", values)
            reset = uow.execute("UPDATE player_dungeon_status SET dungeon_id=?,dungeon_name=?,dungeon_status='not_started',current_layer=0,total_layers=?,last_reset_date=?,reset_generation=?,reset_operation_id=?", (snapshot["dungeon_id"], snapshot["dungeon_name"], snapshot["total_layers"], business_date, generation, operation_id))
            result_json = self._json({"reset_players": int(reset.rowcount), "status": "completed"})
            uow.execute("INSERT INTO dungeon_reset_operations(operation_id,business_date,generation,source,dungeon_snapshot,result_json,status,created_at,updated_at) VALUES(?,?,?,?,?,?, 'completed',?,?)", (operation_id, business_date, generation, source, self._json(snapshot), result_json, now, now))
            row = self._operation(uow, operation_id)
            if row is None:
                raise RuntimeError("dungeon reset operation was not persisted")
            return self._result(row, "applied")

    def operation_result(self, operation_id: str) -> dict[str, Any] | None:
        with DatabaseUnitOfWork(self.database) as uow:
            self.ensure_schema(uow)
            row = self._operation(uow, str(operation_id).strip())
            return None if row is None else self._result(row, "duplicate")

    def ensure_player_status(self, user_id: str, fallback_snapshot: Mapping[str, Any] | None = None) -> dict[str, Any]:
        user_id = str(user_id).strip()
        if not user_id:
            raise ValueError("user_id is required")
        with DatabaseUnitOfWork(self.database, immediate=True) as uow:
            self.ensure_schema(uow)
            global_row = uow.query_one("SELECT dungeon_id,dungeon_name,date,total_layers,dungeon_type,description,reset_generation,reset_operation_id FROM dungeon_global_state WHERE user_id='0'")
            if global_row is None:
                raise RuntimeError("dungeon global state is missing")
            global_state = dict(global_row)
            if int(global_state["total_layers"] or 0) < 1 and fallback_snapshot is not None:
                fallback = self._snapshot(fallback_snapshot)
                if fallback["dungeon_id"] != str(global_state["dungeon_id"] or ""):
                    raise RuntimeError("dungeon global snapshot is incomplete")
                global_state.update({"dungeon_name": fallback["dungeon_name"], "total_layers": fallback["total_layers"], "dungeon_type": fallback["dungeon_type"], "description": fallback["description"]})
                uow.execute("UPDATE dungeon_global_state SET dungeon_name=?,total_layers=?,dungeon_type=?,description=? WHERE user_id='0'", (global_state["dungeon_name"], global_state["total_layers"], global_state["dungeon_type"], global_state["description"]))
            columns = ("dungeon_id","dungeon_name","dungeon_status","current_layer","total_layers","last_reset_date","reset_generation","reset_operation_id")
            row = uow.query_one("SELECT " + ",".join(columns) + " FROM player_dungeon_status WHERE user_id=?", (user_id,))
            values = (str(global_state["dungeon_id"] or ""), str(global_state["dungeon_name"] or ""), "not_started", 0, int(global_state["total_layers"] or 0), str(global_state["date"] or ""), int(global_state["reset_generation"] or 0), str(global_state["reset_operation_id"] or ""))
            if row is None:
                uow.execute("INSERT INTO player_dungeon_status(user_id," + ",".join(columns) + ") VALUES(?" + ",?" * len(columns) + ")", (user_id, *values))
            elif any(str(row[key] or "") != str(values[index]) for index, key in enumerate(columns) if key not in {"current_layer","total_layers","reset_generation"}) or any(int(row[key] or 0) != int(values[index]) for index, key in enumerate(columns) if key in {"current_layer","total_layers","reset_generation"}):
                uow.execute("UPDATE player_dungeon_status SET " + ",".join(f"{key}=?" for key in columns) + " WHERE user_id=?", (*values, user_id))
            result = dict(zip(columns, values))
            return {**result, "current_layer": int(result["current_layer"]), "total_layers": int(result["total_layers"]), "reset_generation": int(result["reset_generation"])}
