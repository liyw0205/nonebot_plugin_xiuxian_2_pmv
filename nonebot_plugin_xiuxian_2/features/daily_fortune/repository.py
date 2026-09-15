from __future__ import annotations

from typing import Any, Mapping

from ...infrastructure.database import DatabaseUnitOfWork


class DailyFortuneRepository:
    def ensure_schema(self, uow: DatabaseUnitOfWork) -> None:
        uow.execute(
            """
            CREATE TABLE IF NOT EXISTS daily_fortune_claims (
                user_id TEXT NOT NULL,
                fortune_date TEXT NOT NULL,
                score INTEGER NOT NULL,
                title TEXT NOT NULL,
                message TEXT NOT NULL,
                operation_id TEXT NOT NULL,
                created_at TEXT NOT NULL,
                PRIMARY KEY(user_id, fortune_date)
            )
            """
        )

    def get(self, uow: DatabaseUnitOfWork, user_id: str, fortune_date: str) -> Mapping[str, Any] | None:
        return uow.query_one(
            "SELECT user_id, fortune_date, score, title, message, operation_id, created_at "
            "FROM daily_fortune_claims WHERE user_id = ? AND fortune_date = ?",
            (user_id, fortune_date),
        )

    def insert(self, uow: DatabaseUnitOfWork, row: Mapping[str, Any]) -> None:
        uow.execute(
            "INSERT INTO daily_fortune_claims(user_id, fortune_date, score, title, message, operation_id, created_at) VALUES (?, ?, ?, ?, ?, ?, ?)",
            (row["user_id"], row["fortune_date"], row["score"], row["title"], row["message"], row["operation_id"], row["created_at"]),
        )


__all__ = ["DailyFortuneRepository"]
