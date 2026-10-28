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

    def my_trades(self, user_id: str, *, limit: int = 5) -> list[dict[str, Any]]: ...


class XianshiQuerySqlRepository:
    """Read Xianshi listings without creating or migrating their schema."""

    def __init__(self, database: str | Path) -> None:
        self.database = Path(database)

    @staticmethod
    def _table_exists(uow: DatabaseUnitOfWork) -> bool:
        return XianshiQuerySqlRepository._has_table(uow, "xianshi_item")

    @staticmethod
    def _has_table(uow: DatabaseUnitOfWork, name: str) -> bool:
        return (
            uow.query_one(
                "SELECT 1 AS present FROM sqlite_master "
                "WHERE type='table' AND name=?",
                (name,),
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

    def my_trades(self, user_id: str, *, limit: int = 5) -> list[dict[str, Any]]:
        """Read the player's recent Xianshi purchases and sales (read-only)."""

        if not self.database.is_file():
            return []
        user_id = str(user_id or "").strip()
        if not user_id:
            return []
        limit = max(int(limit), 0)
        if limit == 0:
            return []
        with DatabaseUnitOfWork(self.database, read_only=True) as uow:
            if not self._has_table(uow, "xianshi_operations"):
                return []
            if self._has_table(uow, "user_xiuxian"):
                buyer_name = (
                    "(SELECT user_name FROM user_xiuxian AS b "
                    "WHERE b.user_id=o.buyer_id ORDER BY b.rowid LIMIT 1)"
                )
                seller_name = (
                    "(SELECT user_name FROM user_xiuxian AS s "
                    "WHERE s.user_id=o.seller_id ORDER BY s.rowid LIMIT 1)"
                )
            else:
                buyer_name = "''"
                seller_name = "''"
            rows = uow.query_all(
                "SELECT o.operation_id,o.listing_id,o.name,o.goods_type,o.quantity,o.total_cost,"
                "o.buyer_id,o.seller_id,o.created_at,"
                f"{buyer_name} AS buyer_name,{seller_name} AS seller_name "
                "FROM xianshi_operations AS o "
                "WHERE o.buyer_id=? OR o.seller_id=? "
                "ORDER BY o.rowid DESC LIMIT ?",
                (user_id, user_id, limit),
            )
        records: list[dict[str, Any]] = []
        for row in rows:
            seller = str(row["seller_id"]) == user_id
            if seller:
                counterparty = str(row["buyer_name"] or row["buyer_id"] or "未知")
            elif str(row["seller_id"]) == "0":
                counterparty = "系统"
            else:
                counterparty = str(
                    row["seller_name"] or row["seller_id"] or "未知"
                )
            records.append(
                {
                    "operation_id": str(row["operation_id"]),
                    "listing_id": str(row["listing_id"]),
                    "name": str(row["name"] or ""),
                    "goods_type": str(row["goods_type"] or ""),
                    "quantity": int(row["quantity"] or 0),
                    "total_cost": int(row["total_cost"] or 0),
                    "role": "seller" if seller else "buyer",
                    "counterparty": counterparty,
                    "created_at": str(row["created_at"] or ""),
                }
            )
        return records


__all__ = ["XianshiQueryRepository", "XianshiQuerySqlRepository"]
