from __future__ import annotations

from typing import Any

from ...infrastructure.database import DatabaseUnitOfWork


class SignInStatisticsRepository:
    """Idempotent statistics projection for committed sign-ins."""

    def __init__(self, database: str) -> None:
        self.database = str(database)

    @staticmethod
    def ensure_schema(uow: DatabaseUnitOfWork) -> None:
        uow.execute(
            "CREATE TABLE IF NOT EXISTS sign_in_statistics_events ("
            "operation_id TEXT PRIMARY KEY, user_id TEXT NOT NULL, event_key TEXT NOT NULL, created_at TEXT NOT NULL)"
        )
        uow.execute(
            "CREATE TABLE IF NOT EXISTS sign_in_statistics_projection ("
            "user_id TEXT NOT NULL, event_key TEXT NOT NULL, value INTEGER NOT NULL DEFAULT 0, "
            "PRIMARY KEY(user_id, event_key))"
        )

    def record(self, *, user_id: str, operation_id: str, event_key: str, occurred_at: Any) -> bool:
        with DatabaseUnitOfWork(self.database, immediate=True) as uow:
            cursor = uow.execute(
                "INSERT INTO sign_in_statistics_events(operation_id,user_id,event_key,created_at) VALUES (?, ?, ?, ?) "
                "ON CONFLICT(operation_id) DO NOTHING",
                (str(operation_id), str(user_id), str(event_key), occurred_at.isoformat()),
            )
            if cursor.rowcount != 1:
                return False
            uow.execute(
                "INSERT INTO sign_in_statistics_projection(user_id,event_key,value) VALUES (?, ?, 1) "
                "ON CONFLICT(user_id,event_key) DO UPDATE SET value=value+1",
                (str(user_id), str(event_key)),
            )
            return True

    def value(self, *, user_id: str, event_key: str) -> int:
        with DatabaseUnitOfWork(self.database) as uow:
            row = uow.query_one(
                "SELECT value FROM sign_in_statistics_projection WHERE user_id=? AND event_key=?",
                (str(user_id), str(event_key)),
            )
            return int(row["value"]) if row else 0


__all__ = ["SignInStatisticsRepository"]
