from __future__ import annotations

from pathlib import Path
from typing import Any, Protocol

from ...infrastructure.database import DatabaseUnitOfWork


class XianshiQueryRepository(Protocol):
    def get_items(
        self,
        *,
        user_id: str | None = None,
        goods_type: str | None = None,
        listing_id: str | None = None,
        name: str | None = None,
    ) -> list[dict[str, Any]]: ...


class XianshiQuerySqlRepository:
    """Read Xianshi listings without creating or migrating their schema."""

    def __init__(self, database: str | Path) -> None:
        self.database = Path(database)

    @staticmethod
    def _table_exists(uow: DatabaseUnitOfWork) -> bool:
        return (
            uow.query_one(
                "SELECT 1 AS present FROM sqlite_master "
                "WHERE type='table' AND name='xianshi_item'"
            )
            is not None
        )

    def get_items(
        self,
        *,
        user_id: str | None = None,
        goods_type: str | None = None,
        listing_id: str | None = None,
        name: str | None = None,
    ) -> list[dict[str, Any]]:
        if not self.database.is_file():
            return []
        filters: list[str] = []
        params: list[str] = []
        for column, value in (
            ("user_id", user_id),
            ("type", goods_type),
            ("id", listing_id),
            ("name", name),
        ):
            if value is not None:
                filters.append(f"{column}=?")
                params.append(str(value))
        with DatabaseUnitOfWork(self.database, read_only=True) as uow:
            if not self._table_exists(uow):
                return []
            sql = "SELECT * FROM xianshi_item"
            if filters:
                sql += " WHERE " + " AND ".join(filters)
            return uow.query_all(sql, params)


__all__ = ["XianshiQueryRepository", "XianshiQuerySqlRepository"]
