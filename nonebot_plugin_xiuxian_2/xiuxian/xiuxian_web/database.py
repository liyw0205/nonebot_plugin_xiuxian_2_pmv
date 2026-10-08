"""Flask adapters for the database-console feature."""

from .core import (
    ACTIVITY_CONFIG_DB,
    ACTIVITY_DB,
    PLAYER_DB,
    TRADE_DB,
    app,
    execute_sql,
    get_database_tables,
    get_db_connection,
    get_dynamic_activity_config_tables,
    get_dynamic_activity_tables,
    get_dynamic_player_tables,
    get_dynamic_trade_tables,
    get_tables,
    jsonify,
    redirect,
    render_template,
    request,
    session,
    sql_ident,
    sql_like_text,
    url_for,
)
from ...features.database_console import DatabaseConsoleApplication, DatabaseConsoleRepository
from ..xiuxian_utils.numeric_bind import format_plain_number


def _database_console_application() -> DatabaseConsoleApplication:
    """Build an adapter with current providers so tests and runtime stay injectable."""
    repository = DatabaseConsoleRepository(
        tables_provider=get_tables,
        dynamic_table_providers=(
            (PLAYER_DB, get_dynamic_player_tables),
            (TRADE_DB, get_dynamic_trade_tables),
            (ACTIVITY_DB, get_dynamic_activity_tables),
            (ACTIVITY_CONFIG_DB, lambda: get_dynamic_activity_config_tables()),
        ),
        database_tables_provider=get_database_tables,
        connection_factory=get_db_connection,
        execute_sql=execute_sql,
        sql_ident=sql_ident,
        sql_like_text=sql_like_text,
    )
    return DatabaseConsoleApplication(repository)


@app.route('/database')
def database():
    if 'admin_id' not in session:
        return redirect(url_for('login'))
    return render_template('database.html', tables=_database_console_application().list_tables())


@app.route('/table/<table_name>', methods=['GET'])
def table_view(table_name):
    if 'admin_id' not in session:
        return redirect(url_for('login'))
    application = _database_console_application()
    db_path, table_info = application.resolve_table(table_name)
    if not db_path:
        return "表不存在", 404
    try:
        page = max(1, int(request.args.get('page', 1)))
    except Exception:
        page = 1
    try:
        per_page = min(200, max(1, int(request.args.get('per_page', 20))))
    except Exception:
        per_page = 20
    search_field = request.args.get('search_field')
    search_value = request.args.get('search_value')
    search_condition = request.args.get('search_condition', '=')
    table_data = application.table_data(
        db_path,
        table_name,
        page=page,
        per_page=per_page,
        search_field=search_field,
        search_value=search_value,
        search_condition=search_condition,
    )
    return render_template(
        'table_view.html',
        table_name=table_name,
        table_info=table_info,
        data=table_data,
        search_field=search_field,
        search_value=search_value,
        search_condition=search_condition,
        primary_key=table_info.get('primary_key', 'id'),
    )


@app.route('/table/<table_name>/<row_id>', methods=['GET', 'POST'])
def row_edit(table_name, row_id):
    if 'admin_id' not in session:
        return redirect(url_for('login'))
    application = _database_console_application()
    db_path, table_info = application.resolve_table(table_name)
    if not db_path:
        return "表不存在", 404
    try:
        primary_conditions, primary_key_fields, is_dynamic_table = application.row_key(
            table_name, table_info, row_id
        )
    except ValueError as exc:
        return str(exc), 400
    is_composite_key = len(primary_key_fields) > 1

    if request.method == 'POST':
        action = request.form.get('action')
        if action == 'update':
            return jsonify(
                application.update_row(
                    db_path,
                    table_name,
                    table_info['fields'],
                    primary_conditions,
                    request.form,
                )
            )
        if action == 'delete':
            return jsonify(application.delete_row(db_path, table_name, primary_conditions))

    row = application.read_row(db_path, table_name, primary_conditions)
    if row is None:
        return "记录不存在", 404
    display_data = {
        key: '' if value is None else format_plain_number(value)
        for key, value in dict(row).items()
    }
    return render_template(
        'row_edit.html',
        table_name=table_name,
        table_info=table_info,
        row_data=display_data,
        primary_key=primary_conditions,
        primary_key_fields=primary_key_fields,
        is_dynamic_table=is_dynamic_table,
        is_composite_key=is_composite_key,
    )


@app.route('/batch_edit/<table_name>', methods=['POST'])
def batch_edit(table_name):
    if 'admin_id' not in session:
        return jsonify({"success": False, "error": "未登录"})
    return jsonify(_database_console_application().batch_edit(table_name, request.form))
