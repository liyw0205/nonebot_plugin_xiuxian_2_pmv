from __future__ import annotations

import csv
import json
from datetime import datetime, timedelta
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
    "trace_id",
    "created_at",
)

FILTER_FIELDS = ("user_id", "sect_id", "source", "action", "trace_id")
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
DEFAULT_ANOMALY_STONE_DELTA = 100000000
QUICK_PRESETS = {
    "today": ("今天", 0),
    "7d": ("近7天", 6),
    "30d": ("近30天", 29),
}


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


def _parse_non_negative_int(raw_value, default=0, max_value=None):
    try:
        value = int(raw_value)
    except (TypeError, ValueError):
        return default
    value = max(value, 0)
    if max_value is not None:
        value = min(value, max_value)
    return value


def _apply_time_preset(filters):
    preset = filters.get("preset")
    if preset not in QUICK_PRESETS:
        filters.pop("preset", None)
        return filters

    if filters.get("start_time") or filters.get("end_time"):
        return filters

    now = datetime.now()
    _, days_back = QUICK_PRESETS[preset]
    start = (now - timedelta(days=days_back)).replace(hour=0, minute=0, second=0, microsecond=0)
    filters["start_time"] = start.strftime("%Y-%m-%d %H:%M:%S")
    filters["end_time"] = now.strftime("%Y-%m-%d %H:%M:%S")
    return filters


def _get_filters(args):
    filters = {}
    for field in (*FILTER_FIELDS, *TIME_FILTER_FIELDS):
        value = str(args.get(field, "")).strip()
        if value:
            filters[field] = value[:100] if field in FILTER_FIELDS else value[:32]
    preset = str(args.get("preset", "")).strip()
    if preset in QUICK_PRESETS:
        filters["preset"] = preset

    min_abs_stone_delta = _parse_non_negative_int(args.get("min_abs_stone_delta"), 0)
    if min_abs_stone_delta > 0:
        filters["min_abs_stone_delta"] = str(min_abs_stone_delta)

    anomaly_stone_delta = _parse_non_negative_int(
        args.get("anomaly_stone_delta"),
        DEFAULT_ANOMALY_STONE_DELTA,
    )
    if str(args.get("anomaly_stone_delta", "")).strip():
        filters["anomaly_stone_delta"] = str(anomaly_stone_delta)

    if str(args.get("has_item_delta", "")).strip() in {"1", "true", "on"}:
        filters["has_item_delta"] = "1"
    if str(args.get("anomaly_only", "")).strip() in {"1", "true", "on"}:
        filters["anomaly_only"] = "1"
    return _apply_time_preset(filters)


def _empty_summary():
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
    if "trace_id" in column_set:
        cur.execute(
            "CREATE INDEX IF NOT EXISTS idx_economy_log_trace_id "
            "ON economy_log(trace_id)"
        )
    if "stone_delta" in column_set:
        cur.execute(
            "CREATE INDEX IF NOT EXISTS idx_economy_log_stone_delta "
            "ON economy_log(stone_delta)"
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

    if filters.get("min_abs_stone_delta") and "stone_delta" in column_set:
        where_parts.append(f"ABS({db_backend.quote_ident('stone_delta')}) >= %s")
        params.append(_parse_non_negative_int(filters.get("min_abs_stone_delta"), 0))

    if filters.get("has_item_delta") and "item_delta" in column_set:
        item_field = db_backend.quote_ident("item_delta")
        where_parts.append(
            f"{item_field} IS NOT NULL "
            f"AND {item_field} <> '' "
            f"AND {item_field} <> '[]' "
            f"AND {item_field} <> '{{}}' "
            f"AND lower({item_field}) <> 'null'"
        )

    if filters.get("anomaly_only"):
        anomaly_parts = []
        if "stone_delta" in column_set:
            anomaly_threshold = _parse_non_negative_int(
                filters.get("anomaly_stone_delta"),
                DEFAULT_ANOMALY_STONE_DELTA,
            )
            anomaly_parts.append(f"ABS({db_backend.quote_ident('stone_delta')}) >= %s")
            params.append(anomaly_threshold)
        if "source" in column_set:
            source_field = db_backend.quote_ident("source")
            anomaly_parts.append(
                f"({source_field} IS NULL OR {source_field} = '' "
                f"OR lower({source_field}) IN ('unknown', 'admin', 'web_admin'))"
            )
        if "action" in column_set:
            action_field = db_backend.quote_ident("action")
            anomaly_parts.append(
                f"({action_field} IS NULL OR {action_field} = '' "
                f"OR lower({action_field}) LIKE '%admin%')"
            )
        if anomaly_parts:
            where_parts.append(f"({' OR '.join(anomaly_parts)})")

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
    if not any(field in column_set for field in (*DELTA_FIELDS, "item_delta", "user_id")):
        return _empty_summary()

    def sum_expr(field):
        if field not in column_set:
            return "0"
        return f"COALESCE(SUM({db_backend.quote_ident(field)}), 0)"

    stone_field = db_backend.quote_ident("stone_delta")
    stone_in_expr = f"COALESCE(SUM(CASE WHEN {stone_field} > 0 THEN {stone_field} ELSE 0 END), 0)" if "stone_delta" in column_set else "0"
    stone_out_expr = f"COALESCE(SUM(CASE WHEN {stone_field} < 0 THEN -{stone_field} ELSE 0 END), 0)" if "stone_delta" in column_set else "0"
    unique_users_expr = "COUNT(DISTINCT user_id)" if "user_id" in column_set else "0"
    if "item_delta" in column_set:
        item_field = db_backend.quote_ident("item_delta")
        item_change_expr = (
            f"COALESCE(SUM(CASE WHEN {item_field} IS NOT NULL "
            f"AND {item_field} <> '' "
            f"AND {item_field} <> '[]' "
            f"AND {item_field} <> '{{}}' "
            f"AND lower({item_field}) <> 'null' THEN 1 ELSE 0 END), 0)"
        )
    else:
        item_change_expr = "0"
    cur = conn.cursor()
    cur.execute(
        f"""
        SELECT
            COUNT(*) AS records,
            {unique_users_expr} AS unique_users,
            {stone_in_expr} AS stone_in,
            {stone_out_expr} AS stone_out,
            {sum_expr("stone_delta")} AS stone_net,
            {sum_expr("exp_delta")} AS exp_total,
            {sum_expr("sect_contribution_delta")} AS sect_contribution_total,
            {sum_expr("sect_scale_delta")} AS sect_scale_total,
            {sum_expr("sect_materials_delta")} AS sect_materials_total,
            {item_change_expr} AS item_change_records
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


def _qualified(field, alias=""):
    quoted = db_backend.quote_ident(field)
    return f"{alias}.{quoted}" if alias else quoted


def _stone_aggregate_exprs(column_set, alias=""):
    if "stone_delta" not in column_set:
        return {
            "stone_in": "0",
            "stone_out": "0",
            "stone_net": "0",
            "gross_stone": "0",
        }
    stone_field = _qualified("stone_delta", alias)
    return {
        "stone_in": f"COALESCE(SUM(CASE WHEN {stone_field} > 0 THEN {stone_field} ELSE 0 END), 0)",
        "stone_out": f"COALESCE(SUM(CASE WHEN {stone_field} < 0 THEN -{stone_field} ELSE 0 END), 0)",
        "stone_net": f"COALESCE(SUM({stone_field}), 0)",
        "gross_stone": f"COALESCE(SUM(ABS({stone_field})), 0)",
    }


def _coerce_stat_rows(rows, int_fields):
    prepared = []
    for row in rows:
        item = dict(row)
        for field in int_fields:
            try:
                item[field] = int(item.get(field) or 0)
            except (TypeError, ValueError):
                item[field] = 0
        prepared.append(item)
    return prepared


def _query_source_stats(conn, where_sql, params, column_set, limit=10):
    if "source" not in column_set and "action" not in column_set:
        return []
    source_expr = db_backend.quote_ident("source") if "source" in column_set else "''"
    action_expr = db_backend.quote_ident("action") if "action" in column_set else "''"
    stone = _stone_aggregate_exprs(column_set)
    cur = conn.cursor()
    cur.execute(
        f"""
        SELECT
            COALESCE({source_expr}, '') AS source,
            COALESCE({action_expr}, '') AS action,
            COUNT(*) AS records,
            {stone["stone_in"]} AS stone_in,
            {stone["stone_out"]} AS stone_out,
            {stone["stone_net"]} AS stone_net,
            {stone["gross_stone"]} AS gross_stone
        FROM {db_backend.quote_ident("economy_log")}
        {where_sql}
        GROUP BY COALESCE({source_expr}, ''), COALESCE({action_expr}, '')
        ORDER BY gross_stone DESC, records DESC
        LIMIT %s
        """,
        [*params, limit],
    )
    return _coerce_stat_rows(
        cur.fetchall(),
        ("records", "stone_in", "stone_out", "stone_net", "gross_stone"),
    )


def _query_user_stats(conn, where_sql, params, column_set, limit=10):
    if "user_id" not in column_set:
        return []
    stone = _stone_aggregate_exprs(column_set, "f")
    has_user_table = conn.table_exists("user_xiuxian") and {"user_id", "user_name"}.issubset(
        set(conn.column_names("user_xiuxian"))
    )
    join_sql = ""
    name_expr = "'' AS user_name"
    if has_user_table:
        join_sql = (
            f"LEFT JOIN {db_backend.quote_ident('user_xiuxian')} u "
            f"ON CAST(f.{db_backend.quote_ident('user_id')} AS TEXT) = CAST(u.{db_backend.quote_ident('user_id')} AS TEXT)"
        )
        name_expr = f"MAX(u.{db_backend.quote_ident('user_name')}) AS user_name"

    cur = conn.cursor()
    cur.execute(
        f"""
        WITH filtered AS (
            SELECT * FROM {db_backend.quote_ident("economy_log")}
            {where_sql}
        )
        SELECT
            f.{db_backend.quote_ident("user_id")} AS user_id,
            {name_expr},
            COUNT(*) AS records,
            {stone["stone_in"]} AS stone_in,
            {stone["stone_out"]} AS stone_out,
            {stone["stone_net"]} AS stone_net,
            {stone["gross_stone"]} AS gross_stone
        FROM filtered f
        {join_sql}
        WHERE f.{db_backend.quote_ident("user_id")} IS NOT NULL
          AND f.{db_backend.quote_ident("user_id")} <> ''
        GROUP BY f.{db_backend.quote_ident("user_id")}
        ORDER BY gross_stone DESC, records DESC
        LIMIT %s
        """,
        [*params, limit],
    )
    return _coerce_stat_rows(
        cur.fetchall(),
        ("records", "stone_in", "stone_out", "stone_net", "gross_stone"),
    )


def _query_sect_stats(conn, where_sql, params, column_set, limit=10):
    if "sect_id" not in column_set:
        return []
    stone = _stone_aggregate_exprs(column_set, "f")
    has_sect_table = conn.table_exists("sects") and {"sect_id", "sect_name"}.issubset(set(conn.column_names("sects")))
    join_sql = ""
    name_expr = "'' AS sect_name"
    if has_sect_table:
        join_sql = (
            f"LEFT JOIN {db_backend.quote_ident('sects')} s "
            f"ON f.{db_backend.quote_ident('sect_id')} = s.{db_backend.quote_ident('sect_id')}"
        )
        name_expr = f"MAX(s.{db_backend.quote_ident('sect_name')}) AS sect_name"

    cur = conn.cursor()
    cur.execute(
        f"""
        WITH filtered AS (
            SELECT * FROM {db_backend.quote_ident("economy_log")}
            {where_sql}
        )
        SELECT
            f.{db_backend.quote_ident("sect_id")} AS sect_id,
            {name_expr},
            COUNT(*) AS records,
            {stone["stone_in"]} AS stone_in,
            {stone["stone_out"]} AS stone_out,
            {stone["stone_net"]} AS stone_net,
            {stone["gross_stone"]} AS gross_stone
        FROM filtered f
        {join_sql}
        WHERE f.{db_backend.quote_ident("sect_id")} IS NOT NULL
          AND f.{db_backend.quote_ident("sect_id")} <> ''
        GROUP BY f.{db_backend.quote_ident("sect_id")}
        ORDER BY gross_stone DESC, records DESC
        LIMIT %s
        """,
        [*params, limit],
    )
    return _coerce_stat_rows(
        cur.fetchall(),
        ("records", "stone_in", "stone_out", "stone_net", "gross_stone"),
    )


def _query_large_rows(conn, select_fields, where_sql, params, column_set, limit=15):
    if "stone_delta" not in column_set:
        return []
    order_parts = [f"ABS({db_backend.quote_ident('stone_delta')}) DESC"]
    if "created_at" in column_set:
        order_parts.append(f"{db_backend.quote_ident('created_at')} DESC")
    if "id" in column_set:
        order_parts.append(f"{db_backend.quote_ident('id')} DESC")
    rows = _query_rows(
        conn,
        select_fields,
        where_sql,
        params,
        " ORDER BY " + ", ".join(order_parts),
        page_size=limit,
        offset=0,
    )
    return [_prepare_row(row) for row in rows]


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
            "source_stats": _query_source_stats(conn, where_sql, params, column_set),
            "user_stats": _query_user_stats(conn, where_sql, params, column_set),
            "sect_stats": _query_sect_stats(conn, where_sql, params, column_set),
            "large_rows": _query_large_rows(conn, select_fields, where_sql, params, column_set),
            "quick_presets": QUICK_PRESETS,
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
