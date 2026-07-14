from __future__ import annotations

import json
from contextlib import closing
from dataclasses import dataclass, field
from pathlib import Path
from threading import RLock

from ..xiuxian_utils import db_backend


@dataclass(frozen=True)
class TribulationStateMigrationResult:
    status: str
    state: dict = field(default_factory=dict)

    @property
    def database_ready(self) -> bool:
        return self.status in {
            "applied",
            "duplicate",
            "database_authoritative",
        }


class TribulationStateMigrationService:
    """Import one legacy tribulation state without overwriting database state."""

    _STATE_FIELDS = (
        "current_rate",
        "heart_devil_count",
        "last_time",
        "next_level",
    )

    def __init__(self, game_database: str | Path, lock: RLock | None = None) -> None:
        self._game_database = Path(game_database)
        self._lock = lock or RLock()

    @staticmethod
    def normalize(data, *, base_rate=30) -> dict:
        data = dict(data or {})
        try:
            current_rate = int(data.get("current_rate", base_rate))
        except (TypeError, ValueError):
            current_rate = int(base_rate)
        try:
            heart_devil_count = int(data.get("heart_devil_count", 0))
        except (TypeError, ValueError):
            heart_devil_count = 0
        return {
            "current_rate": current_rate,
            "heart_devil_count": heart_devil_count,
            "last_time": data.get("last_time") or None,
            "next_level": data.get("next_level") or None,
        }

    @classmethod
    def _row_state(cls, row, *, base_rate=30) -> dict:
        return cls.normalize(
            {
                "current_rate": row[0],
                "heart_devil_count": row[1],
                "last_time": row[2],
                "next_level": row[3],
            },
            base_rate=base_rate,
        )

    @staticmethod
    def _ensure_schema(conn) -> None:
        conn.execute(
            "CREATE TABLE IF NOT EXISTS tribulation_state_migration_operations("
            "operation_id TEXT PRIMARY KEY,user_id TEXT NOT NULL UNIQUE,"
            "payload TEXT NOT NULL,created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP)"
        )

    def migrate(self, operation_id, user_id, legacy_data, *, base_rate=30):
        operation_id = str(operation_id).strip()
        user_id = str(user_id).strip()
        if not operation_id or not user_id:
            raise ValueError("operation_id and user_id must not be empty")
        state = self.normalize(legacy_data, base_rate=base_rate)
        payload = json.dumps(
            state,
            ensure_ascii=True,
            sort_keys=True,
            separators=(",", ":"),
        )

        with self._lock, closing(db_backend.connect(self._game_database)) as conn:
            try:
                conn.execute("BEGIN IMMEDIATE")
                self._ensure_schema(conn)
                previous = conn.execute(
                    "SELECT user_id,payload FROM tribulation_state_migration_operations "
                    "WHERE operation_id=%s",
                    (operation_id,),
                ).fetchone()
                if previous is not None:
                    conn.rollback()
                    status = (
                        "duplicate"
                        if str(previous[0]) == user_id and str(previous[1]) == payload
                        else "operation_conflict"
                    )
                    saved = json.loads(str(previous[1]))
                    return TribulationStateMigrationResult(status, saved)

                current = conn.execute(
                    "SELECT current_rate,heart_devil_count,last_time,next_level "
                    "FROM user_tribulation WHERE user_id=%s",
                    (user_id,),
                ).fetchone()
                if current is not None:
                    conn.rollback()
                    return TribulationStateMigrationResult(
                        "database_authoritative",
                        self._row_state(current, base_rate=base_rate),
                    )

                conn.execute(
                    "INSERT INTO user_tribulation("
                    "user_id,current_rate,heart_devil_count,last_time,next_level"
                    ") VALUES(%s,%s,%s,%s,%s)",
                    (
                        user_id,
                        state["current_rate"],
                        state["heart_devil_count"],
                        state["last_time"],
                        state["next_level"],
                    ),
                )
                conn.execute(
                    "INSERT INTO tribulation_state_migration_operations("
                    "operation_id,user_id,payload) VALUES(%s,%s,%s)",
                    (operation_id, user_id, payload),
                )
                conn.commit()
                return TribulationStateMigrationResult("applied", state)
            except Exception:
                conn.rollback()
                raise


__all__ = [
    "TribulationStateMigrationResult",
    "TribulationStateMigrationService",
]
