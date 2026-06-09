from __future__ import annotations

import csv
import json
from io import StringIO

from .core import (
    DATABASE,
    Response,
    app,
    db_backend,
    get_db_connection,
    redirect,
    render_template,
    request,
    session,
    url_for,
)

ECONOMY_LOG_FIELDS = (
    "id",
    "user_id",
    "sect_id",
    "source",
    "action",
    "stone_delta",
    "exp_delta",
    "sect_contribution_delta",
    "sect_scale_delta",
    "sect_materials_delta",
    "item_delta",
    "detail",
    "created_at",
)

FILTER_FIELDS = ("user_id", "sect_id", "source", "action")
TIME_FILTER_FIELDS = ("start_time", "end_time")
DELTA_FIELDS = (
    "stone_delta",
    "exp_delta",
    "sect_contribution_delta",
    "sect_scale_delta",
    "sect_materials_delta",
)
DEFAULT_PAGE_SIZE = 100
MAX_PAGE_SIZE = 500


def _parse_positive_int(raw_value, default, min_value=1, max_value=None):
    try:
        value = int(raw_value)
    except (TypeError, ValueError):
        return default
    value = max(value, min_value)
    if max_value is not None:
        value = min(value, max_value)
    return value


def _parse_page_args(args):
    page = _parse_positive_int(args.get("page", 1), 1)
    raw_page_size = args.get("page_size", args.get("limit", DEFAULT_PAGE_SIZE))
    page_size = _parse_positive_int(raw_page_size, DEFAULT_PAGE_SIZE, 1, MAX_PAGE_SIZE)
    return page, page_size


def _get_filters(args):
    filters = {}
    for field in (*FILTER_FIELDS, *TIME_FILTER_FIELDS):
        value = str(args.get(field, "")).strip()
        if value:
            filters[field] = value[:100] if field in FILTER_FIELDS else value[:32]
    return filters


def _empty_summary():
    return {
        "stone_in": 0,
        "stone_out": 0,
        "exp_total": 0,
        "sect_contribution_total": 0,
        "sect_scale_total": 0,
        "sect_materials_total": 0,
    }


def _empty_result(filters, page, page_size, notice=None, error=None):
    return {
        "rows": [],
        "filters": filters,
        "page": page,
        "page_size": page_size,
        "limit": page_size,
        "total": 0,
        "total_pages": 1,
        "has_prev": False,
        "has_next": False,
        "summary": _empty_summary(),
        "source_options": [],
        "action_options": [],
        "notice": notice,
        "error": error,
    }


def _format_json_cell(value):
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


def _prepare_row(row):
    prepared = dict(row)
    for field in DELTA_FIELDS:
        try:
            prepared[field] = int(prepared.get(field) or 0)
        except (TypeError, ValueError):
            prepared[field] = 0
        prepared[f"{field}_class"] = (
            "delta-positive" if prepared[field] > 0 else "delta-negative" if prepared[field] < 0 else "delta-zero"
        )
    prepared["item_delta_json"] = _format_json_cell(prepared.get("item_delta", ""))
    prepared["detail_json"] = _format_json_cell(prepared.get("detail", ""))
    return prepared


def _ensure_economy_log_indexes(conn, column_set):
    cur = conn.cursor()
    if "created_at" in column_set:
        cur.execute(
            "CREATE INDEX IF NOT EXISTS idx_economy_log_created_at "
            "ON economy_log(created_at)"
        )
    if {"source", "action", "created_at"}.issubset(column_set):
        cur.execute(
            "CREATE INDEX IF NOT EXISTS idx_economy_log_source_action_time "
            "ON economy_log(source, action, created_at)"
        )
    if {"user_id", "source", "created_at"}.issubset(column_set):
        cur.execute(
            "CREATE INDEX IF NOT EXISTS idx_economy_log_user_source_time "
            "ON economy_log(user_id, source, created_at)"
        )
    conn.commit()


def _build_where(filters, column_set):
    where_parts = []
    params = []
    for field in FILTER_FIELDS:
        value = filters.get(field)
        if value and field in column_set:
            where_parts.append(f"{db_backend.quote_ident(field)} = %s")
            params.append(value)

    if filters.get("start_time") and "created_at" in column_set:
        where_parts.append(f"{db_backend.quote_ident('created_at')} >= %s")
        params.append(filters["start_time"])
    if filters.get("end_time") and "created_at" in column_set:
        where_parts.append(f"{db_backend.quote_ident('created_at')} <= %s")
        params.append(filters["end_time"])

    where_sql = f" WHERE {' AND '.join(where_parts)}" if where_parts else ""
    return where_sql, params


def _query_distinct_options(conn, column_set, field):
    if field not in column_set:
        return []
    cur = conn.cursor()
    cur.execute(
        f"""
        SELECT DISTINCT {db_backend.quote_ident(field)} AS value
        FROM {db_backend.quote_ident("economy_log")}
        WHERE {db_backend.quote_ident(field)} IS NOT NULL
          AND {db_backend.quote_ident(field)} <> ''
        ORDER BY {db_backend.quote_ident(field)} ASC
        LIMIT 200
        """
    )
    return [str(row["value"] if hasattr(row, "keys") else row[0]) for row in cur.fetchall()]


def _query_summary(conn, where_sql, params, column_set):
    if not any(field in column_set for field in DELTA_FIELDS):
        return _empty_summary()

    def sum_expr(field):
        if field not in column_set:
            return "0"
        return f"COALESCE(SUM({db_backend.quote_ident(field)}), 0)"

    stone_field = db_backend.quote_ident("stone_delta")
    stone_in_expr = f"COALESCE(SUM(CASE WHEN {stone_field} > 0 THEN {stone_field} ELSE 0 END), 0)" if "stone_delta" in column_set else "0"
    stone_out_expr = f"COALESCE(SUM(CASE WHEN {stone_field} < 0 THEN -{stone_field} ELSE 0 END), 0)" if "stone_delta" in column_set else "0"
    cur = conn.cursor()
    cur.execute(
        f"""
        SELECT
            {stone_in_expr} AS stone_in,
            {stone_out_expr} AS stone_out,
            {sum_expr("exp_delta")} AS exp_total,
            {sum_expr("sect_contribution_delta")} AS sect_contribution_total,
            {sum_expr("sect_scale_delta")} AS sect_scale_total,
            {sum_expr("sect_materials_delta")} AS sect_materials_total
        FROM {db_backend.quote_ident("economy_log")}
        {where_sql}
        """,
        params,
    )
    row = cur.fetchone()
    if not row:
        return _empty_summary()
    row_dict = dict(row)
    return {key: int(row_dict.get(key) or 0) for key in _empty_summary()}


def _query_total(conn, where_sql, params):
    cur = conn.cursor()
    cur.execute(
        f"SELECT COUNT(*) AS total FROM {db_backend.quote_ident('economy_log')}{where_sql}",
        params,
    )
    row = cur.fetchone()
    return int((dict(row).get("total") if row else 0) or 0)


def _get_order_sql(column_set):
    order_fields = [field for field in ("created_at", "id") if field in column_set]
    if not order_fields:
        return ""
    return " ORDER BY " + ", ".join(f"{db_backend.quote_ident(field)} DESC" for field in order_fields)


def _query_rows(conn, select_fields, where_sql, params, order_sql, page_size=None, offset=None):
    fields_sql = ", ".join(db_backend.quote_ident(field) for field in select_fields)
    sql = f"SELECT {fields_sql} FROM {db_backend.quote_ident('economy_log')}{where_sql}{order_sql}"
    query_params = list(params)
    if page_size is not None:
        sql += " LIMIT %s OFFSET %s"
        query_params.extend([page_size, offset or 0])
    cur = conn.cursor()
    cur.execute(sql, query_params)
    return [dict(row) for row in cur.fetchall()]


def _query_economy_logs(filters, page, page_size):
    if not db_backend.database_exists(DATABASE):
        return _empty_result(filters, page, page_size, notice="修仙数据库不存在，暂无经济流水。")

    conn = get_db_connection(DATABASE)
    try:
        if not conn.table_exists("economy_log"):
            return _empty_result(filters, page, page_size, notice="economy_log 表尚未创建，暂无经济流水。")

        columns = conn.column_names("economy_log")
        column_set = set(columns)
        _ensure_economy_log_indexes(conn, column_set)
        select_fields = [field for field in ECONOMY_LOG_FIELDS if field in column_set]
        if not select_fields:
            return _empty_result(filters, page, page_size, error="economy_log 表字段异常，无法展示。")

        where_sql, params = _build_where(filters, column_set)
        total = _query_total(conn, where_sql, params)
        total_pages = max((total + page_size - 1) // page_size, 1)
        page = min(max(page, 1), total_pages)
        offset = (page - 1) * page_size
        rows = _query_rows(
            conn,
            select_fields,
            where_sql,
            params,
            _get_order_sql(column_set),
            page_size=page_size,
            offset=offset,
        )
        return {
            "rows": [_prepare_row(row) for row in rows],
            "filters": filters,
            "page": page,
            "page_size": page_size,
            "limit": page_size,
            "total": total,
            "total_pages": total_pages,
            "has_prev": page > 1,
            "has_next": page < total_pages,
            "summary": _query_summary(conn, where_sql, params, column_set),
            "source_options": _query_distinct_options(conn, column_set, "source"),
            "action_options": _query_distinct_options(conn, column_set, "action"),
            "notice": None,
            "error": None,
        }
    except Exception as exc:
        return _empty_result(filters, page, page_size, error=f"查询经济流水失败：{exc}")
    finally:
        conn.close()


def _build_query_args(filters, page_size, page=None):
    args = {field: value for field, value in filters.items() if value}
    args["page_size"] = page_size
    if page is not None:
        args["page"] = page
    return args


def _query_export_rows(filters):
    if not db_backend.database_exists(DATABASE):
        return [], "修仙数据库不存在，暂无经济流水。"

    conn = get_db_connection(DATABASE)
    try:
        if not conn.table_exists("economy_log"):
            return [], "economy_log 表尚未创建，暂无经济流水。"
        columns = conn.column_names("economy_log")
        column_set = set(columns)
        select_fields = [field for field in ECONOMY_LOG_FIELDS if field in column_set]
        if not select_fields:
            return [], "economy_log 表字段异常，无法导出。"
        where_sql, params = _build_where(filters, column_set)
        rows = _query_rows(conn, select_fields, where_sql, params, _get_order_sql(column_set))
        return rows, None
    finally:
        conn.close()


def _csv_response(rows):
    output = StringIO()
    writer = csv.DictWriter(output, fieldnames=ECONOMY_LOG_FIELDS, extrasaction="ignore")
    writer.writeheader()
    for row in rows:
        writer.writerow({field: row.get(field, "") for field in ECONOMY_LOG_FIELDS})
    return Response(
        output.getvalue(),
        mimetype="text/csv; charset=utf-8",
        headers={"Content-Disposition": "attachment; filename=economy_logs.csv"},
    )


@app.route("/economy_logs")
def economy_logs():
    if "admin_id" not in session:
        return redirect(url_for("login"))

    page, page_size = _parse_page_args(request.args)
    filters = _get_filters(request.args)
    result = _query_economy_logs(filters, page, page_size)
    result["query_args"] = _build_query_args(result["filters"], result["page_size"], result["page"])
    result["first_page_args"] = _build_query_args(result["filters"], result["page_size"], 1)
    result["prev_page_args"] = _build_query_args(result["filters"], result["page_size"], max(result["page"] - 1, 1))
    result["next_page_args"] = _build_query_args(
        result["filters"],
        result["page_size"],
        min(result["page"] + 1, result["total_pages"]),
    )
    result["last_page_args"] = _build_query_args(result["filters"], result["page_size"], result["total_pages"])
    result["export_args"] = _build_query_args(result["filters"], result["page_size"], result["page"])
    return render_template("economy_logs.html", result=result)


@app.route("/economy_logs/export")
def economy_logs_export():
    if "admin_id" not in session:
        return redirect(url_for("login"))

    filters = _get_filters(request.args)
    rows, error = _query_export_rows(filters)
    if error:
        rows = []
    return _csv_response(rows)
