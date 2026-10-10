from __future__ import annotations

import json
from pathlib import Path
from typing import Any, Iterator, Mapping

from ...infrastructure.database import DatabaseUnitOfWork
from .schemas import (
    DEFAULT_ANOMALY_STONE_DELTA,
    DELTA_FIELDS,
    ECONOMY_LOG_FIELDS,
    FILTER_FIELDS,
    QUICK_PRESETS,
)
_BASE_COLUMNS = frozenset(ECONOMY_LOG_FIELDS)


def _quote(identifier: str) -> str:
    if identifier not in _BASE_COLUMNS | {"user_name", "sect_name"}:
        raise ValueError(f"unsupported economy ledger column: {identifier}")
    return f'"{identifier}"'


def _empty_summary() -> dict[str, int]:
    return {
        "records": 0,
        "unique_users": 0,
        "stone_in": 0,
        "stone_out": 0,
        "stone_net": 0,
        "exp_total": 0,
        "sect_contribution_total": 0,
        "sect_scale_total": 0,
        "sect_materials_total": 0,
        "item_change_records": 0,
    }


def _empty_result(
    filters: Mapping[str, str], page: int, page_size: int, *, notice: str | None = None,
    error: str | None = None,
) -> dict[str, Any]:
    return {
        "rows": [],
        "filters": dict(filters),
        "page": page,
        "page_size": page_size,
        "limit": page_size,
        "total": 0,
        "total_pages": 1,
        "has_prev": False,
        "has_next": False,
        "summary": _empty_summary(),
        "source_stats": [],
        "user_stats": [],
        "sect_stats": [],
        "large_rows": [],
        "quick_presets": QUICK_PRESETS,
        "source_options": [],
        "action_options": [],
        "notice": notice,
        "error": error,
    }


def _format_json_cell(value: Any) -> dict[str, Any]:
    raw_text = "" if value is None else str(value)
    try:
        parsed = json.loads(raw_text) if raw_text else None
    except (TypeError, ValueError):
        return {
            "raw": raw_text,
            "display": raw_text,
            "is_json": False,
            "is_long": len(raw_text) > 80,
        }
    display = json.dumps(parsed, ensure_ascii=False, indent=2)
    return {
        "raw": raw_text,
        "display": display,
        "is_json": True,
        "is_long": len(display) > 160 or "\n" in display,
    }


def _prepare_row(row: Mapping[str, Any]) -> dict[str, Any]:
    prepared = dict(row)
    for field in DELTA_FIELDS:
        try:
            prepared[field] = int(prepared.get(field) or 0)
        except (TypeError, ValueError):
            prepared[field] = 0
        prepared[f"{field}_class"] = (
            "delta-positive" if prepared[field] > 0 else
            "delta-negative" if prepared[field] < 0 else "delta-zero"
        )
    prepared["item_delta_json"] = _format_json_cell(prepared.get("item_delta", ""))
    prepared["detail_json"] = _format_json_cell(prepared.get("detail", ""))
    return prepared


class EconomyLedgerSqlRepository:
    """Read the shared economy ledger without schema or index side effects."""

    def __init__(self, database: str | Path) -> None:
        self.database = Path(database)

    @staticmethod
    def _table_exists(uow: DatabaseUnitOfWork, table: str) -> bool:
        return uow.query_one(
            "SELECT 1 AS present FROM sqlite_master WHERE type='table' AND name=?",
            (table,),
        ) is not None

    @staticmethod
    def _columns(uow: DatabaseUnitOfWork, table: str) -> set[str]:
        return {
            str(row["name"]).casefold()
            for row in uow.query_all(f'PRAGMA table_info("{table}")')
        }

    @staticmethod
    def _build_where(filters: Mapping[str, str], columns: set[str]) -> tuple[str, list[Any]]:
        where_parts: list[str] = []
        params: list[Any] = []
        for field in FILTER_FIELDS:
            value = filters.get(field)
            if value and field in columns:
                where_parts.append(f"{_quote(field)} = ?")
                params.append(value)

        for field, operator in (("start_time", ">="), ("end_time", "<=")):
            if filters.get(field) and "created_at" in columns:
                where_parts.append(f'{_quote("created_at")} {operator} ?')
                params.append(filters[field])

        if filters.get("min_abs_stone_delta") and "stone_delta" in columns:
            where_parts.append('ABS("stone_delta") >= ?')
            params.append(max(int(filters["min_abs_stone_delta"]), 0))

        if filters.get("has_item_delta") and "item_delta" in columns:
            where_parts.append(
                '"item_delta" IS NOT NULL AND "item_delta" <> \'\' '
                'AND "item_delta" <> \'[]\' AND "item_delta" <> \'{}\' '
                "AND lower(\"item_delta\") <> 'null'"
            )

        if filters.get("anomaly_only"):
            anomaly_parts = []
            if "stone_delta" in columns:
                try:
                    threshold = max(int(filters.get("anomaly_stone_delta", DEFAULT_ANOMALY_STONE_DELTA)), 0)
                except (TypeError, ValueError):
                    threshold = DEFAULT_ANOMALY_STONE_DELTA
                anomaly_parts.append('ABS("stone_delta") >= ?')
                params.append(threshold)
            if "source" in columns:
                anomaly_parts.append(
                    '("source" IS NULL OR "source" = \'\' '
                    "OR lower(\"source\") IN ('unknown', 'admin', 'web_admin'))"
                )
            if "action" in columns:
                anomaly_parts.append(
                    '("action" IS NULL OR "action" = \'\' '
                    "OR lower(\"action\") LIKE '%admin%')"
                )
            if anomaly_parts:
                where_parts.append(f"({' OR '.join(anomaly_parts)})")

        return (f" WHERE {' AND '.join(where_parts)}" if where_parts else "", params)

    @staticmethod
    def _order_sql(columns: set[str]) -> str:
        fields = [field for field in ("created_at", "id") if field in columns]
        return " ORDER BY " + ", ".join(f'{_quote(field)} DESC' for field in fields) if fields else ""

    @staticmethod
    def _rows(
        uow: DatabaseUnitOfWork, columns: list[str], where_sql: str, params: list[Any],
        order_sql: str, *, limit: int | None = None, offset: int = 0,
    ) -> list[dict[str, Any]]:
        fields_sql = ", ".join(_quote(field) for field in columns)
        sql = f'SELECT {fields_sql} FROM "economy_log"{where_sql}{order_sql}'
        query_params = list(params)
        if limit is not None:
            sql += " LIMIT ? OFFSET ?"
            query_params.extend((limit, offset))
        return uow.query_all(sql, query_params)

    @staticmethod
    def _distinct_options(uow: DatabaseUnitOfWork, columns: set[str], field: str) -> list[str]:
        if field not in columns:
            return []
        rows = uow.query_all(
            f'SELECT DISTINCT {_quote(field)} AS value FROM "economy_log" '
            f'WHERE {_quote(field)} IS NOT NULL AND {_quote(field)} <> \'\' '
            f'ORDER BY {_quote(field)} ASC LIMIT 200'
        )
        return [str(row["value"]) for row in rows]

    @staticmethod
    def _summary(
        uow: DatabaseUnitOfWork, where_sql: str, params: list[Any], columns: set[str]
    ) -> dict[str, int]:
        if not any(field in columns for field in (*DELTA_FIELDS, "item_delta", "user_id")):
            return _empty_summary()

        def sum_expr(field: str) -> str:
            return f'COALESCE(SUM({_quote(field)}), 0)' if field in columns else "0"

        stone_in = (
            'COALESCE(SUM(CASE WHEN "stone_delta" > 0 THEN "stone_delta" ELSE 0 END), 0)'
            if "stone_delta" in columns else "0"
        )
        stone_out = (
            'COALESCE(SUM(CASE WHEN "stone_delta" < 0 THEN -"stone_delta" ELSE 0 END), 0)'
            if "stone_delta" in columns else "0"
        )
        unique_users = "COUNT(DISTINCT user_id)" if "user_id" in columns else "0"
        item_change = (
            "COALESCE(SUM(CASE WHEN \"item_delta\" IS NOT NULL AND \"item_delta\" <> '' "
            "AND \"item_delta\" <> '[]' AND \"item_delta\" <> '{}' "
            "AND lower(\"item_delta\") <> 'null' THEN 1 ELSE 0 END), 0)"
            if "item_delta" in columns else "0"
        )
        row = uow.query_one(
            f'SELECT COUNT(*) AS records,{unique_users} AS unique_users,'
            f'{stone_in} AS stone_in,{stone_out} AS stone_out,{sum_expr("stone_delta")} AS stone_net,'
            f'{sum_expr("exp_delta")} AS exp_total,'
            f'{sum_expr("sect_contribution_delta")} AS sect_contribution_total,'
            f'{sum_expr("sect_scale_delta")} AS sect_scale_total,'
            f'{sum_expr("sect_materials_delta")} AS sect_materials_total,'
            f'{item_change} AS item_change_records FROM "economy_log"{where_sql}', params
        )
        if row is None:
            return _empty_summary()
        return {key: int(row.get(key) or 0) for key in _empty_summary()}

    @staticmethod
    def _stone_exprs(columns: set[str], alias: str = "") -> dict[str, str]:
        if "stone_delta" not in columns:
            return {"stone_in": "0", "stone_out": "0", "stone_net": "0", "gross_stone": "0"}
        stone = f'{alias}.{_quote("stone_delta")}' if alias else _quote("stone_delta")
        return {
            "stone_in": f"COALESCE(SUM(CASE WHEN {stone} > 0 THEN {stone} ELSE 0 END), 0)",
            "stone_out": f"COALESCE(SUM(CASE WHEN {stone} < 0 THEN -{stone} ELSE 0 END), 0)",
            "stone_net": f"COALESCE(SUM({stone}), 0)",
            "gross_stone": f"COALESCE(SUM(ABS({stone})), 0)",
        }

    @staticmethod
    def _coerce_stats(rows: list[Mapping[str, Any]], int_fields: tuple[str, ...]) -> list[dict[str, Any]]:
        result = []
        for row in rows:
            item = dict(row)
            for field in int_fields:
                try:
                    item[field] = int(item.get(field) or 0)
                except (TypeError, ValueError):
                    item[field] = 0
            result.append(item)
        return result

    @classmethod
    def _source_stats(
        cls, uow: DatabaseUnitOfWork, where_sql: str, params: list[Any], columns: set[str], limit: int = 10
    ) -> list[dict[str, Any]]:
        if "source" not in columns and "action" not in columns:
            return []
        source = _quote("source") if "source" in columns else "''"
        action = _quote("action") if "action" in columns else "''"
        stone = cls._stone_exprs(columns)
        rows = uow.query_all(
            f'SELECT COALESCE({source}, \'\') AS source,COALESCE({action}, \'\') AS action,'
            f'COUNT(*) AS records,{stone["stone_in"]} AS stone_in,{stone["stone_out"]} AS stone_out,'
            f'{stone["stone_net"]} AS stone_net,{stone["gross_stone"]} AS gross_stone '
            f'FROM "economy_log"{where_sql} '
            f'GROUP BY COALESCE({source}, \'\'),COALESCE({action}, \'\') '
            "ORDER BY gross_stone DESC,records DESC LIMIT ?",
            [*params, limit],
        )
        return cls._coerce_stats(rows, ("records", "stone_in", "stone_out", "stone_net", "gross_stone"))

    @classmethod
    def _user_stats(
        cls, uow: DatabaseUnitOfWork, where_sql: str, params: list[Any], columns: set[str], limit: int = 10
    ) -> list[dict[str, Any]]:
        if "user_id" not in columns:
            return []
        stone = cls._stone_exprs(columns, "f")
        has_user_table = cls._table_exists(uow, "user_xiuxian") and {
            "user_id", "user_name"
        }.issubset(cls._columns(uow, "user_xiuxian"))
        join = (
            'LEFT JOIN "user_xiuxian" u ON CAST(f."user_id" AS TEXT)=CAST(u."user_id" AS TEXT)'
            if has_user_table else ""
        )
        name = 'MAX(u."user_name") AS user_name' if has_user_table else "'' AS user_name"
        rows = uow.query_all(
            'WITH filtered AS (SELECT * FROM "economy_log"' + where_sql + ") "
            f'SELECT f."user_id" AS user_id,{name},COUNT(*) AS records,'
            f'{stone["stone_in"]} AS stone_in,{stone["stone_out"]} AS stone_out,'
            f'{stone["stone_net"]} AS stone_net,{stone["gross_stone"]} AS gross_stone '
            f'FROM filtered f {join} WHERE f."user_id" IS NOT NULL AND f."user_id" <> \'\' '
            'GROUP BY f."user_id" ORDER BY gross_stone DESC,records DESC LIMIT ?',
            [*params, limit],
        )
        return cls._coerce_stats(rows, ("records", "stone_in", "stone_out", "stone_net", "gross_stone"))

    @classmethod
    def _sect_stats(
        cls, uow: DatabaseUnitOfWork, where_sql: str, params: list[Any], columns: set[str], limit: int = 10
    ) -> list[dict[str, Any]]:
        if "sect_id" not in columns:
            return []
        stone = cls._stone_exprs(columns, "f")
        has_sect_table = cls._table_exists(uow, "sects") and {
            "sect_id", "sect_name"
        }.issubset(cls._columns(uow, "sects"))
        join = 'LEFT JOIN "sects" s ON f."sect_id"=s."sect_id"' if has_sect_table else ""
        name = 'MAX(s."sect_name") AS sect_name' if has_sect_table else "'' AS sect_name"
        rows = uow.query_all(
            'WITH filtered AS (SELECT * FROM "economy_log"' + where_sql + ") "
            f'SELECT f."sect_id" AS sect_id,{name},COUNT(*) AS records,'
            f'{stone["stone_in"]} AS stone_in,{stone["stone_out"]} AS stone_out,'
            f'{stone["stone_net"]} AS stone_net,{stone["gross_stone"]} AS gross_stone '
            f'FROM filtered f {join} WHERE f."sect_id" IS NOT NULL AND f."sect_id" <> \'\' '
            'GROUP BY f."sect_id" ORDER BY gross_stone DESC,records DESC LIMIT ?',
            [*params, limit],
        )
        return cls._coerce_stats(rows, ("records", "stone_in", "stone_out", "stone_net", "gross_stone"))

    @classmethod
    def _large_rows(
        cls, uow: DatabaseUnitOfWork, selected: list[str], where_sql: str,
        params: list[Any], columns: set[str], limit: int = 15,
    ) -> list[dict[str, Any]]:
        if "stone_delta" not in columns:
            return []
        order = ['ABS("stone_delta") DESC']
        order.extend(f'{_quote(field)} DESC' for field in ("created_at", "id") if field in columns)
        rows = cls._rows(uow, selected, where_sql, params, " ORDER BY " + ", ".join(order), limit=limit)
        return [_prepare_row(row) for row in rows]

    def query_page(self, filters: Mapping[str, str], page: int, page_size: int) -> dict[str, Any]:
        if not self.database.is_file():
            return _empty_result(filters, page, page_size, notice="修仙数据库不存在，暂无经济流水。")
        try:
            with DatabaseUnitOfWork(self.database, read_only=True) as uow:
                if not self._table_exists(uow, "economy_log"):
                    return _empty_result(filters, page, page_size, notice="economy_log 表尚未创建，暂无经济流水。")
                columns = self._columns(uow, "economy_log")
                selected = [field for field in ECONOMY_LOG_FIELDS if field in columns]
                if not selected:
                    return _empty_result(filters, page, page_size, error="economy_log 表字段异常，无法展示。")

                where_sql, params = self._build_where(filters, columns)
                summary_fields = (*DELTA_FIELDS, "item_delta", "user_id")
                if any(field in columns for field in summary_fields):
                    summary = self._summary(uow, where_sql, params, columns)
                    total = summary["records"]
                else:
                    total_row = uow.query_one(
                        f'SELECT COUNT(*) AS total FROM "economy_log"{where_sql}', params
                    )
                    total = int((total_row or {}).get("total") or 0)
                    summary = _empty_summary()
                total_pages = max((total + page_size - 1) // page_size, 1)
                page = min(max(page, 1), total_pages)
                rows = self._rows(
                    uow, selected, where_sql, params, self._order_sql(columns),
                    limit=page_size, offset=(page - 1) * page_size,
                )
                return {
                    "rows": [_prepare_row(row) for row in rows],
                    "filters": dict(filters),
                    "page": page,
                    "page_size": page_size,
                    "limit": page_size,
                    "total": total,
                    "total_pages": total_pages,
                    "has_prev": page > 1,
                    "has_next": page < total_pages,
                    "summary": summary,
                    "source_stats": self._source_stats(uow, where_sql, params, columns),
                    "user_stats": self._user_stats(uow, where_sql, params, columns),
                    "sect_stats": self._sect_stats(uow, where_sql, params, columns),
                    "large_rows": self._large_rows(uow, selected, where_sql, params, columns),
                    "quick_presets": QUICK_PRESETS,
                    "source_options": self._distinct_options(uow, columns, "source"),
                    "action_options": self._distinct_options(uow, columns, "action"),
                    "notice": None,
                    "error": None,
                }
        except Exception as exc:
            return _empty_result(filters, page, page_size, error=f"查询经济流水失败：{exc}")

    def iter_export_rows(self, filters: Mapping[str, str]) -> Iterator[dict[str, Any]]:
        if not self.database.is_file():
            return
        with DatabaseUnitOfWork(self.database, read_only=True) as uow:
            if not self._table_exists(uow, "economy_log"):
                return
            columns = self._columns(uow, "economy_log")
            selected = [field for field in ECONOMY_LOG_FIELDS if field in columns]
            if not selected:
                return
            where_sql, params = self._build_where(filters, columns)
            sql = (
                f'SELECT {", ".join(_quote(field) for field in selected)} '
                f'FROM "economy_log"{where_sql}{self._order_sql(columns)}'
            )
            cursor = uow.execute(sql, params)
            while True:
                batch = cursor.fetchmany(256)
                if not batch:
                    break
                for row in batch:
                    record = dict(row)
                    yield {field: record.get(field, "") for field in ECONOMY_LOG_FIELDS}


__all__ = ["ECONOMY_LOG_FIELDS", "EconomyLedgerSqlRepository"]
