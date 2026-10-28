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

    @staticmethod
    def daily_schema_exists(uow: DatabaseUnitOfWork) -> bool:
        return uow.query_one(
            "SELECT 1 AS present FROM sqlite_master WHERE type='table' AND name='sign_in_daily'"
        ) is not None

    def daily_signed(
        self, uow: DatabaseUnitOfWork, user_id: str, business_date: str
    ) -> bool | None:
        """Return today's receipt state; ``None`` means the table is absent."""

        if not self.daily_schema_exists(uow):
            return None
        row = uow.query_one(
            "SELECT 1 AS present FROM sign_in_daily WHERE user_id=? AND business_date=?",
            (str(user_id), str(business_date)),
        )
        return row is not None

    def record_daily(
        self,
        uow: DatabaseUnitOfWork,
        user_id: str,
        business_date: str,
        stone: int,
        operation_id: str,
    ) -> bool:
        """Insert the daily receipt; a duplicate must roll the claim back."""

        if not self.daily_schema_exists(uow):
            return False
        cursor = uow.execute(
            "INSERT OR IGNORE INTO sign_in_daily"
            "(user_id,business_date,stone,operation_id) VALUES(?,?,?,?)",
            (str(user_id), str(business_date), int(stone), str(operation_id)),
        )
        if int(cursor.rowcount) != 1:
            raise RuntimeError("sign_in_daily receipt conflict")
        return True

    def daily_dates(
        self,
        uow: DatabaseUnitOfWork,
        user_id: str,
        start_date: str,
        end_date: str,
    ) -> list[str] | None:
        """Return signed dates in a range; ``None`` means the table is absent."""

        if not self.daily_schema_exists(uow):
            return None
        rows = uow.query_all(
            "SELECT business_date FROM sign_in_daily "
            "WHERE user_id=? AND business_date BETWEEN ? AND ? ORDER BY business_date",
            (str(user_id), str(start_date), str(end_date)),
        )
        return [str(row["business_date"]) for row in rows]
