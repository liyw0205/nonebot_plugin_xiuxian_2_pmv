"""Read-only ownership for the shared player profile projection."""

from __future__ import annotations

from pathlib import Path
from typing import Any

from ...core.numeric import normalize_user_row
from ...infrastructure.database import DatabaseUnitOfWork


MAX_USER_SEARCH_QUERY_CHARS = 64
MAX_USER_SEARCH_RESULTS = 10


class PlayerProfileSqlRepository:
    """Read the legacy ``user_xiuxian`` projection without opening a writer."""

    def __init__(self, database: str | Path) -> None:
        self.database = str(database)

    def get_user_profile(self, user_id: int | str) -> dict[str, Any] | None:
        try:
            with DatabaseUnitOfWork(self.database, read_only=True) as uow:
                row = uow.query_one(
                    "SELECT * FROM user_xiuxian WHERE user_id=? "
                    "ORDER BY rowid ASC LIMIT 1",
                    (str(user_id),),
                )
        except Exception:
            # A missing database/schema is not a reason to create one during a
            # command.  Callers treat the closed projection as an unknown user.
            return None
        return normalize_user_row(row)

    def get_user_profile_by_name(self, user_name: str) -> dict[str, Any] | None:
        try:
            with DatabaseUnitOfWork(self.database, read_only=True) as uow:
                row = uow.query_one(
                    "SELECT * FROM user_xiuxian WHERE user_name=? "
                    "ORDER BY rowid ASC LIMIT 1",
                    (str(user_name),),
                )
        except Exception:
            return None
        return normalize_user_row(row)

    @staticmethod
    def _escape_like(value: str) -> str:
        return value.replace("\\", "\\\\").replace("%", "\\%").replace("_", "\\_")

    def search_users(self, query: str) -> list[dict[str, Any]]:
        query = str(query)
        if len(query) > MAX_USER_SEARCH_QUERY_CHARS:
            raise ValueError(f"query must be at most {MAX_USER_SEARCH_QUERY_CHARS} characters")
        pattern = f"%{self._escape_like(query)}%"
        try:
            with DatabaseUnitOfWork(self.database, read_only=True) as uow:
                rows = uow.query_all(
                    "SELECT user_id, user_name FROM user_xiuxian "
                    "WHERE COALESCE(CAST(user_name AS TEXT), '') LIKE ? ESCAPE '\\' "
                    "ORDER BY rowid ASC LIMIT ?",
                    (pattern, MAX_USER_SEARCH_RESULTS),
                )
        except Exception:
            # Missing files/schemas must not trigger setup during a read.
            return []
        return [
            {"id": row.get("user_id"), "name": row.get("user_name")}
            for row in rows
        ]


__all__ = [
    "MAX_USER_SEARCH_QUERY_CHARS",
    "MAX_USER_SEARCH_RESULTS",
    "PlayerProfileSqlRepository",
]
