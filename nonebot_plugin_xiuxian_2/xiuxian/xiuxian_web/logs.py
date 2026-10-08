from ...features.logs import LogsApplication
from ...infrastructure.database import DatabaseUnitOfWork
from ..xiuxian_utils.message_db import get_message_db_path
from .core import DATABASE, app, jsonify, redirect, render_template, request, runtime_clock, session, url_for
from .messages import (
    _prepare_message_rows as _prepare_web_message_rows,
    build_user_avatar_url,
)


logs_application = LogsApplication(
    DATABASE,
    get_message_db_path(),
    __file__,
    user_avatar_builder=build_user_avatar_url,
    year_provider=lambda: runtime_clock.now().year,
)


@app.route('/logs')
def logs():
    if 'admin_id' not in session:
        return redirect(url_for('login'))
    return render_template('logs.html')


@app.route('/api/logs/users')
def api_logs_users():
    if 'admin_id' not in session:
        return jsonify({"success": False, "error": "未登录"})
    return jsonify(logs_application.users(
        query=request.args.get("query", "").strip(),
        limit=request.args.get("limit", 20),
    ))


@app.route('/api/logs/user_messages')
def api_logs_user_messages():
    if 'admin_id' not in session:
        return jsonify({"success": False, "error": "未登录"})

    result = logs_application.user_messages(
        user_id=request.args.get("user_id", "").strip(),
        scene=request.args.get("scene", "ALL").strip(),
        direction=request.args.get("direction", "ALL").strip(),
        keyword=request.args.get("keyword", "").strip(),
        adapter=request.args.get("adapter", "").strip(),
        start=request.args.get("start", "").strip(),
        end=request.args.get("end", "").strip(),
        page=request.args.get("page", 1),
        page_size=request.args.get("page_size", 200),
    )
    if result.get("success"):
        try:
            with DatabaseUnitOfWork(logs_application.message_database, read_only=True) as uow:
                result["rows"] = _prepare_web_message_rows(result["rows"], conn=uow.connection)
        except Exception as exc:
            result = {"success": False, "error": f"获取用户消息失败：{exc}"}
    return jsonify(result)


@app.route('/api/logs/files')
def api_logs_files():
    if 'admin_id' not in session:
        return jsonify({"success": False, "error": "未登录"})
    return jsonify(logs_application.files())


@app.route('/api/logs/read')
def api_logs_read():
    if 'admin_id' not in session:
        return jsonify({"success": False, "error": "未登录"})
    return jsonify(logs_application.read(
        file=request.args.get("file", "").strip(),
        keyword=request.args.get("keyword", "").strip(),
        level=request.args.get("level", "ALL").strip(),
        start=request.args.get("start", "").strip(),
        end=request.args.get("end", "").strip(),
        page=request.args.get("page", 1),
        page_size=request.args.get("page_size", 200),
    ))


@app.route('/api/logs/tail')
def api_logs_tail():
    if 'admin_id' not in session:
        return jsonify({"success": False, "error": "未登录"})
    return jsonify(logs_application.tail(
        file=request.args.get("file", "").strip(),
        offset=request.args.get("offset", 0),
        keyword=request.args.get("keyword", "").strip(),
        level=request.args.get("level", "ALL").strip(),
        start=request.args.get("start", "").strip(),
        end=request.args.get("end", "").strip(),
        ignore_unknown=request.args.get("ignore_unknown", "0") == "1",
        ignore_keywords=request.args.get("ignore_keywords", "").strip(),
    ))
