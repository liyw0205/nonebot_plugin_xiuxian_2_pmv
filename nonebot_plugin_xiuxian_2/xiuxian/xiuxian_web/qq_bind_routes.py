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
            "connect_url": bind_page_url(task_id),
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
    completed = _tasks.completed(task_id)
    if completed is not None:
        return jsonify(completed)
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
        replaced = merge_qq_bots_env(_env_file(), appid, secret)
    except Exception as exc:
        return jsonify(
            {
                "success": False,
                "status": "error",
                "error": f"写入 QQ_BOTS 失败：{exc}",
            }
        ), 500
    response = {
        "success": True,
        "status": "completed",
        "appid": appid,
        "replaced": replaced,
        "message": "QQ 已确认绑定，配置已安全落盘。重启后将仅通过该机器人建立 WebSocket 连接。",
    }
    _tasks.complete(task_id, response)
    return jsonify(response)


@app.route("/api/config/qq-bind/restart-capability")
def qq_bind_restart_capability():
    denied = _require_admin()
    if denied:
        return denied
    capability = detect_restart(Path.cwd())
    return jsonify(
        {
            "success": True,
            "automatic": bool(capability.get("automatic")),
            "mode": capability.get("mode"),
            "message": capability.get("message"),
        }
    )


@app.route("/api/config/qq-bind/restart", methods=["POST"])
def qq_bind_restart():
    denied = _require_admin()
    if denied:
        return denied
    payload = request.get_json(silent=True) or {}
    if payload.get("confirm") is not True:
        return jsonify({"success": False, "error": "需要明确确认重启"}), 400
    capability = detect_restart(Path.cwd())
    if not capability.get("automatic"):
        return jsonify(
            {
                "success": False,
                "automatic": False,
                "mode": capability.get("mode"),
                "error": capability.get("message"),
            }
        ), 409
    if not schedule_restart(capability):
        return jsonify({"success": False, "error": "无法提交重启任务"}), 500
    return jsonify(
        {
            "success": True,
            "scheduled": True,
            "message": "重启任务已提交，页面连接将暂时中断。",
        }
    )
