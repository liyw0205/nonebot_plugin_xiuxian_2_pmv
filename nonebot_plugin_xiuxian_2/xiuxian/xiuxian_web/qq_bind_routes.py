from __future__ import annotations

from pathlib import Path

from nonebot import get_driver

from ...features.qq_bind import QqBindApplication
from .core import app, jsonify, request, run_async, send_file, session
from .qq_bind import (
    BindTaskStore,
    bind_page_url,
    create_bind_task,
    decrypt_bind_secret,
    merge_qq_bots_env,
    poll_bind_result,
    qr_png_bytes,
)

from .qq_restart import detect_restart, schedule_restart

_tasks = BindTaskStore(ttl=600)


def _require_admin():
    if "admin_id" not in session:
        return jsonify({"success": False, "error": "未登录"}), 401
    return None


def _env_file() -> Path:
    driver = get_driver()
    env_file = getattr(driver.config, "_env_file", None)
    if isinstance(env_file, (str, Path)):
        path = Path(env_file)
        if not path.is_absolute():
            path = Path.cwd() / path
        return path
    for name in (".env.dev", ".env"):
        candidate = Path.cwd() / name
        if candidate.exists():
            return candidate
    return Path.cwd() / ".env.dev"


_qq_bind_application = QqBindApplication(
    tasks=_tasks,
    create_task=create_bind_task,
    poll_task=poll_bind_result,
    bind_page_url=bind_page_url,
    qr_png_bytes=qr_png_bytes,
    decrypt_secret=decrypt_bind_secret,
    merge_env=merge_qq_bots_env,
    env_file=_env_file,
    detect_restart=detect_restart,
    schedule_restart=schedule_restart,
    project_root=Path.cwd,
)


@app.route("/api/config/qq-bind/start", methods=["POST"])
def qq_bind_start():
    denied = _require_admin()
    if denied:
        return denied
    result = run_async(_qq_bind_application.start())
    return jsonify(result.body), result.status_code


@app.route("/api/config/qq-bind/qr/<task_id>")
def qq_bind_qr(task_id: str):
    denied = _require_admin()
    if denied:
        return denied
    image = _qq_bind_application.qr_png(task_id)
    if image is None:
        return jsonify({"success": False, "error": "绑定任务不存在或已过期"}), 404
    from io import BytesIO

    return send_file(BytesIO(image), mimetype="image/png")


@app.route("/api/config/qq-bind/poll", methods=["POST"])
def qq_bind_poll():
    denied = _require_admin()
    if denied:
        return denied
    payload = request.get_json(silent=True) or {}
    result = run_async(_qq_bind_application.poll(str(payload.get("task_id") or "")))
    return jsonify(result.body), result.status_code


@app.route("/api/config/qq-bind/restart-capability")
def qq_bind_restart_capability():
    denied = _require_admin()
    if denied:
        return denied
    return jsonify(_qq_bind_application.restart_capability())


@app.route("/api/config/qq-bind/restart", methods=["POST"])
def qq_bind_restart():
    denied = _require_admin()
    if denied:
        return denied
    payload = request.get_json(silent=True) or {}
    result = _qq_bind_application.restart(payload.get("confirm"))
    return jsonify(result.body), result.status_code
