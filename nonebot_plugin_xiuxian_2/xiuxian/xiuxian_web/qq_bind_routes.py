from __future__ import annotations

import base64
import secrets
from pathlib import Path

from nonebot import get_driver

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


@app.route("/api/config/qq-bind/start", methods=["POST"])
def qq_bind_start():
    denied = _require_admin()
    if denied:
        return denied
    key = base64.b64encode(secrets.token_bytes(32)).decode("ascii")
    result = run_async(create_bind_task(key))
    task_id = str((result.get("data") or {}).get("task_id") or "")
    if result.get("retcode") != 0 or not task_id:
        return jsonify(
            {
                "success": False,
                "error": result.get("msg") or "创建扫码绑定任务失败",
            }
        ), 502
    _tasks.add(task_id, key)
    return jsonify(
        {
            "success": True,
            "task_id": task_id,
            "status": "waiting",
            "qr_url": f"/api/config/qq-bind/qr/{task_id}",
        }
    )


@app.route("/api/config/qq-bind/qr/<task_id>")
def qq_bind_qr(task_id: str):
    denied = _require_admin()
    if denied:
        return denied
    if _tasks.get(task_id) is None:
        return jsonify({"success": False, "error": "绑定任务不存在或已过期"}), 404
    from io import BytesIO

    return send_file(BytesIO(qr_png_bytes(bind_page_url(task_id))), mimetype="image/png")


@app.route("/api/config/qq-bind/poll", methods=["POST"])
def qq_bind_poll():
    denied = _require_admin()
    if denied:
        return denied
    payload = request.get_json(silent=True) or {}
    task_id = str(payload.get("task_id") or "")
    entry = _tasks.get(task_id)
    if not task_id or entry is None:
        return jsonify({"success": True, "status": "expired"})

    result = run_async(poll_bind_result(task_id))
    if result.get("retcode") != 0:
        return jsonify(
            {
                "success": False,
                "status": "error",
                "error": result.get("msg") or "查询绑定结果失败",
            }
        )
    data = result.get("data") or {}
    status = data.get("status")
    if status == 3:
        _tasks.pop(task_id)
        return jsonify({"success": True, "status": "expired"})
    if status != 2:
        return jsonify({"success": True, "status": "waiting"})

    entry = _tasks.pop(task_id)
    appid = str(data.get("bot_appid") or "")
    encrypted = str(data.get("bot_encrypt_secret") or "")
    if entry is None or not appid or not encrypted:
        return jsonify(
            {
                "success": False,
                "status": "error",
                "error": "绑定结果缺少 AppID 或 Secret",
            }
        )
    try:
        secret = decrypt_bind_secret(encrypted, entry[1])
        created = merge_qq_bots_env(_env_file(), appid, secret)
    except Exception as exc:
        return jsonify(
            {
                "success": False,
                "status": "error",
                "error": f"写入 QQ_BOTS 失败：{exc}",
            }
        ), 500
    return jsonify(
        {
            "success": True,
            "status": "completed",
            "appid": appid,
            "created": created,
            "message": "QQ 官方机器人已写入 .env.dev，重启后将通过 WebSocket 连接。",
        }
    )
