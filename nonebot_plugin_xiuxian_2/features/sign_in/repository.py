from __future__ import annotations

from typing import Any

from ...infrastructure.database import DatabaseUnitOfWork
from .domain import SignInRecord


class SignInRepository:
    """Repository for the game database's sign-in projection."""

    def ensure_schema(self, uow: DatabaseUnitOfWork) -> None:
        uow.execute(
            """
            CREATE TABLE IF NOT EXISTS sign_in_operations (
                operation_id TEXT PRIMARY KEY,
                user_id TEXT NOT NULL,
                stone INTEGER NOT NULL,
                created_at TEXT DEFAULT CURRENT_TIMESTAMP
            )
            """
        )

    def operation(self, uow: DatabaseUnitOfWork, operation_id: str) -> SignInRecord | None:
        self.ensure_schema(uow)
        row = uow.query_one(
            "SELECT operation_id, user_id, stone FROM sign_in_operations WHERE operation_id = ?",
            (operation_id,),
        )
        return SignInRecord(str(row["user_id"]), str(row["operation_id"]), int(row["stone"])) if row else None

    def user_sign_states(self, uow: DatabaseUnitOfWork, user_id: str) -> list[int]:
        rows = uow.query_all(
            "SELECT CAST(COALESCE(is_sign, 0) AS INTEGER) AS is_sign FROM user_xiuxian WHERE user_id = ?",
            (user_id,),
        )
        return [int(row["is_sign"] or 0) for row in rows]

    def user_snapshot(self, uow: DatabaseUnitOfWork, user_id: str) -> dict[str, int]:
        rows = uow.query_all(
            "SELECT CAST(COALESCE(is_sign, 0) AS INTEGER) AS is_sign, "
            "CAST(COALESCE(stone, 0) AS REAL) AS stone FROM user_xiuxian WHERE user_id = ?",
            (user_id,),
        )
        return {
            "rows": len(rows),
            "signed_rows": sum(int(row["is_sign"] or 0) for row in rows),
            "stone": sum(int(float(row["stone"] or 0)) for row in rows),
        }

    def apply(self, uow: DatabaseUnitOfWork, user_id: str, stone: int) -> int:
        cursor = uow.execute(
            "UPDATE user_xiuxian SET is_sign = 1, "
            "stone = CAST(COALESCE(stone, 0) AS REAL) + CAST(? AS REAL) "
            "WHERE user_id = ? AND CAST(COALESCE(is_sign, 0) AS INTEGER) = 0",
            (stone, user_id),
        )
        return int(cursor.rowcount)

    def insert_operation(self, uow: DatabaseUnitOfWork, record: SignInRecord) -> None:
        uow.execute(
            "INSERT INTO sign_in_operations(operation_id, user_id, stone) VALUES (?, ?, ?)",
            (record.operation_id, record.user_id, record.stone),
        )
