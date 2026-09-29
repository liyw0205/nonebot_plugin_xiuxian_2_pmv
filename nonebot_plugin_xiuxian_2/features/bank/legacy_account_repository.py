from __future__ import annotations

from typing import Any

from ...infrastructure.database import DatabaseUnitOfWork


class BankLegacyAccountReadRepository:
    def record_status(self, uow: DatabaseUnitOfWork, user_id: str) -> str:
        table = uow.query_one(
            "SELECT 1 AS present FROM sqlite_master WHERE type='table' AND name='bankinfo'"
        )
        if table is None:
            return "missing"
        columns = {
            str(row["name"]).casefold()
            for row in uow.query_all('PRAGMA table_info("bankinfo")')
        }
        if "user_id" not in columns:
            return "invalid"
        row = uow.query_one('SELECT 1 AS present FROM "bankinfo" WHERE user_id=?', (str(user_id),))
        return "present" if row is not None else "missing"

    def read_fields(self, uow: DatabaseUnitOfWork, user_id: str) -> dict[str, Any] | None:
        table = uow.query_one(
            "SELECT 1 AS present FROM sqlite_master WHERE type='table' AND name='bankinfo'"
        )
        if table is None:
            return None
        columns = {
            str(row["name"]).casefold()
            for row in uow.query_all('PRAGMA table_info("bankinfo")')
        }
        if "user_id" not in columns:
            return None
        return uow.query_one('SELECT * FROM "bankinfo" WHERE user_id=?', (str(user_id),))


__all__ = ["BankLegacyAccountReadRepository"]
