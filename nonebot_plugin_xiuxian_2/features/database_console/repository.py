from __future__ import annotations

from collections.abc import Callable, Iterable, Mapping
from typing import Any

from ...xiuxian.xiuxian_utils.numeric_bind import format_plain_number
from .schemas import (
    DEFAULT_PRIMARY_KEY,
    DEFAULT_TABLE_PAGE_SIZE,
    LIKE_SEARCH_OPERATOR,
    MAX_RANGE_SEARCH_VALUES,
    MAX_TABLE_PAGE_SIZE,
    MIN_TABLE_PAGE_SIZE,
    RANGE_SEARCH_OPERATORS,
)


class DatabaseConsoleRepository:
    """SQL adapter used by the database-console application.

    The web module supplies the existing database providers, keeping this
    feature independent from Flask while preserving the console's SQLite
    compatibility helpers and response shapes.
    """

    def __init__(
        self,
        *,
        tables_provider: Callable[[], Mapping[str, Any]],
        dynamic_table_providers: Iterable[tuple[Any, Callable[[], Mapping[str, Any]]]],
        database_tables_provider: Callable[[Any], Mapping[str, Any]],
        connection_factory: Callable[[Any], Any],
        execute_sql: Callable[[Any, str, Any], Any],
        sql_ident: Callable[[str], str],
        sql_like_text: Callable[[str], str],
    ) -> None:
        self._tables_provider = tables_provider
        self._dynamic_table_providers = tuple(dynamic_table_providers)
        self._database_tables_provider = database_tables_provider
        self._connection_factory = connection_factory
        self._execute_sql = execute_sql
        self._sql_ident = sql_ident
        self._sql_like_text = sql_like_text

    def all_tables(self) -> Mapping[str, Any]:
        return self._tables_provider()

    def quote_identifier(self, name: str) -> str:
        return self._sql_ident(name)

    def like_text(self, field: str) -> str:
        return self._sql_like_text(field)

    def resolve_table(self, table_name: str) -> tuple[Any, dict[str, Any] | None]:
        for db_info in self.all_tables().values():
            tables = db_info.get("tables", {})
            if table_name in tables:
                return db_info.get("path"), tables[table_name]
        for db_path, provider in self._dynamic_table_providers:
            tables = provider()
            if table_name in tables:
                return db_path, tables[table_name]
        return None, None

    def table_data(
        self,
        db_path: Any,
        table_name: str,
        *,
        page: int = 1,
        per_page: int = DEFAULT_TABLE_PAGE_SIZE,
        search_field: str | None = None,
        search_value: str | None = None,
        search_condition: str = LIKE_SEARCH_OPERATOR,
    ) -> dict[str, Any]:
        try:
            page = max(MIN_TABLE_PAGE_SIZE, int(page))
        except Exception:
            page = 1
        try:
            per_page = min(MAX_TABLE_PAGE_SIZE, max(MIN_TABLE_PAGE_SIZE, int(per_page)))
        except Exception:
            per_page = DEFAULT_TABLE_PAGE_SIZE
        offset = (page - 1) * per_page

        table_info = self._database_tables_provider(db_path).get(table_name, {})
        if not table_info:
            return self._empty_result("表不存在", page, per_page)

        primary_key = table_info.get("primary_key", DEFAULT_PRIMARY_KEY)
        primary_keys = set(primary_key if isinstance(primary_key, list) else [primary_key])
        fields = table_info.get("fields", [])
        if not fields:
            return self._empty_result("表中没有字段", page, per_page)
        if search_field and search_field not in fields:
            return self._empty_result("搜索字段不存在", page, per_page)

        table_sql = self._sql_ident(table_name)
        sql = f"SELECT *, COUNT(*) OVER() AS total_count FROM {table_sql}"
        params: list[Any] = []
        where_clauses: list[str] = []

        if search_field and search_value:
            if search_condition == LIKE_SEARCH_OPERATOR:
                values = search_value.split()
                if len(values) > 1:
                    where_clauses.append(
                        "(" + " OR ".join(self._sql_like_text(search_field) for _ in values) + ")"
                    )
                    params.extend(f"%{value}%" for value in values)
                else:
                    where_clauses.append(self._sql_like_text(search_field))
                    params.append(f"%{search_value}%")
            elif search_condition in RANGE_SEARCH_OPERATORS:
                values = search_value.split()
                if len(values) > MAX_RANGE_SEARCH_VALUES:
                    return self._empty_result("搜索值过多", page, per_page)
                if len(values) == 1:
                    if not search_value.replace(".", "", 1).isdigit():
                        return self._empty_result("搜索值必须是数值", page, per_page)
                    where_clauses.append(
                        f"{self._sql_ident(search_field)} {search_condition} %s"
                    )
                    params.append(float(values[0]))
                else:
                    if not values[0].replace(".", "", 1).isdigit():
                        return self._empty_result("第一个搜索值必须是数值", page, per_page)
                    if not values[1]:
                        return self._empty_result("第二个搜索值不能为空", page, per_page)
                    where_clauses.append(
                        f"{self._sql_ident(search_field)} {search_condition} %s"
                    )
                    searchable_fields = [field for field in fields if field not in primary_keys]
                    where_clauses.append(
                        "(" + " OR ".join(self._sql_like_text(field) for field in searchable_fields) + ")"
                    )
                    params.extend([float(values[0])] + [f"%{values[1]}%" for _ in searchable_fields])
            else:
                return self._empty_result("无效的搜索条件", page, per_page)
        elif search_value and not search_field:
            searchable_fields = [field for field in fields if field not in primary_keys]
            if searchable_fields:
                where_clauses.append(
                    "(" + " OR ".join(self._sql_like_text(field) for field in searchable_fields) + ")"
                )
                params.extend(f"%{search_value}%" for _ in searchable_fields)
            else:
                where_clauses.append("1=0")

        if where_clauses:
            sql += " WHERE " + " AND ".join(where_clauses)
        sql += " LIMIT %s OFFSET %s"
        params.extend([per_page, offset])

        try:
            conn = self._connection_factory(db_path)
            try:
                cursor = conn.cursor()
                cursor.execute(sql, params)
                rows = cursor.fetchall()
            finally:
                conn.close()
            if not rows:
                return {"data": [], "total": 0, "page": page, "per_page": per_page, "total_pages": 0}
            total = rows[0]["total_count"]
            data = []
            for row in rows:
                item = dict(row)
                item.pop("total_count", None)
                for key, value in list(item.items()):
                    if value is not None:
                        item[key] = format_plain_number(value)
                data.append(item)
            return {
                "data": data,
                "total": total,
                "page": page,
                "per_page": per_page,
                "total_pages": (total + per_page - 1) // per_page,
            }
        except Exception as exc:
            return self._empty_result(str(exc), page, per_page)

    def execute(self, db_path: Any, sql: str, params: list[Any]) -> Any:
        return self._execute_sql(db_path, sql, params)

    def table_exists(self, db_path: Any, table_name: str) -> bool:
        conn = self._connection_factory(db_path)
        try:
            return bool(conn.table_exists(table_name))
        finally:
            conn.close()

    @staticmethod
    def _empty_result(error: str | None, page: int, per_page: int) -> dict[str, Any]:
        result = {
            "data": [],
            "total": 0,
            "page": page,
            "per_page": per_page,
            "total_pages": 0,
        }
        if error:
            result["error"] = error
        return result
