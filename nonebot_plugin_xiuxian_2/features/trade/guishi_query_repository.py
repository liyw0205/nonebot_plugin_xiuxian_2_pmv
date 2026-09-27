from __future__ import annotations

import json
from pathlib import Path
from typing import Any, Protocol

from ...infrastructure.database import DatabaseUnitOfWork


class GuishiQueryRepository(Protocol):
    def get_orders(
        self,
        *,
        user_id: str | None = None,
        name: str | None = None,
        order_type: str | None = None,
        order_id: str | None = None,
    ) -> list[dict[str, Any]] | None: ...

    def get_account(self, user_id: str) -> tuple[int, dict[str, Any]]: ...


class GuishiQuerySqlRepository:
    """Read Guishi projections without opening write transactions or creating schema."""

    def __init__(self, database: str | Path) -> None:
        self.database = Path(database)

    @staticmethod
    def _table_exists(uow: DatabaseUnitOfWork, table: str) -> bool:
        return (
            uow.query_one(
                "SELECT 1 AS present FROM sqlite_master "
                "WHERE type='table' AND name=?",
                (table,),
            )
            is not None
        )

    def get_orders(
        self,
        *,
        user_id: str | None = None,
        name: str | None = None,
        order_type: str | None = None,
        order_id: str | None = None,
    ) -> list[dict[str, Any]] | None:
        if not self.database.is_file():
            return None

        filters: list[str] = []
        params: list[str] = []
        if user_id is not None:
            filters.append("user_id=?")
            params.append(str(user_id))
        if name:
            filters.append("item_name=?")
            params.append(str(name))
        if order_type:
            if order_type == "qiugou":
                filters.append("(item_type=? OR item_type=?)")
                params.extend(("qiugou", "求购"))
            elif order_type == "baitan":
                filters.append("(item_type=? OR item_type=?)")
                params.extend(("baitan", "摆摊"))
            else:
                filters.append("item_type=?")
                params.append(str(order_type))
        if order_id:
            filters.append("id=?")
            params.append(str(order_id))

        with DatabaseUnitOfWork(self.database, read_only=True) as uow:
            if not self._table_exists(uow, "guishi_item"):
                return None
            sql = "SELECT * FROM guishi_item"
            if filters:
                sql += " WHERE " + " AND ".join(filters)
            rows = uow.query_all(sql, params)
            return rows or None

    def get_account(self, user_id: str) -> tuple[int, dict[str, Any]]:
        if not self.database.is_file():
            return 0, {}

        with DatabaseUnitOfWork(self.database, read_only=True) as uow:
            if not self._table_exists(uow, "guishi_info"):
                return 0, {}
            row = uow.query_one(
                "SELECT stored_stone,items FROM guishi_info WHERE user_id=?",
                (str(user_id),),
            )
        if row is None:
            return 0, {}
        try:
            items = json.loads(row.get("items") or "{}")
        except (TypeError, ValueError):
            items = {}
        return int(row.get("stored_stone") or 0), items if isinstance(items, dict) else {}


__all__ = ["GuishiQueryRepository", "GuishiQuerySqlRepository"]
