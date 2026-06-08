from __future__ import annotations

from .core import (
    DATABASE,
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
DEFAULT_LIMIT = 100
MAX_LIMIT = 500


def _parse_limit(raw_limit):
    try:
        limit = int(raw_limit)
    except (TypeError, ValueError):
        return DEFAULT_LIMIT
    return min(max(limit, 1), MAX_LIMIT)


def _get_filters(args):
    filters = {}
    for field in FILTER_FIELDS:
        value = str(args.get(field, "")).strip()
        if value:
            filters[field] = value[:100]
    return filters


def _empty_result(filters, limit, notice=None, error=None):
    return {
        "rows": [],
        "filters": filters,
        "limit": limit,
        "notice": notice,
        "error": error,
    }


def _query_economy_logs(filters, limit):
    if not db_backend.database_exists(DATABASE):
        return _empty_result(filters, limit, notice="修仙数据库不存在，暂无经济流水。")

    conn = get_db_connection(DATABASE)
    try:
        if not conn.table_exists("economy_log"):
            return _empty_result(filters, limit, notice="economy_log 表尚未创建，暂无经济流水。")

        columns = conn.column_names("economy_log")
        column_set = set(columns)
        select_fields = [field for field in ECONOMY_LOG_FIELDS if field in column_set]
        if not select_fields:
            return _empty_result(filters, limit, error="economy_log 表字段异常，无法展示。")

        where_parts = []
        params = []
        for field, value in filters.items():
            if field not in column_set:
                continue
            where_parts.append(f"{db_backend.quote_ident(field)} = %s")
            params.append(value)

        where_sql = f" WHERE {' AND '.join(where_parts)}" if where_parts else ""
        order_fields = [field for field in ("created_at", "id") if field in column_set]
        order_sql = ""
        if order_fields:
            order_sql = " ORDER BY " + ", ".join(
                f"{db_backend.quote_ident(field)} DESC" for field in order_fields
            )

        fields_sql = ", ".join(db_backend.quote_ident(field) for field in select_fields)
        table_sql = db_backend.quote_ident("economy_log")
        sql = f"SELECT {fields_sql} FROM {table_sql}{where_sql}{order_sql} LIMIT %s"
        params.append(limit)

        cur = conn.cursor()
        cur.execute(sql, params)
        rows = [dict(row) for row in cur.fetchall()]
        return {
            "rows": rows,
            "filters": filters,
            "limit": limit,
            "notice": None,
            "error": None,
        }
    except Exception as exc:
        return _empty_result(filters, limit, error=f"查询经济流水失败：{exc}")
    finally:
        conn.close()


@app.route("/economy_logs")
def economy_logs():
    if "admin_id" not in session:
        return redirect(url_for("login"))

    limit = _parse_limit(request.args.get("limit", DEFAULT_LIMIT))
    filters = _get_filters(request.args)
    result = _query_economy_logs(filters, limit)
    return render_template("economy_logs.html", result=result)
