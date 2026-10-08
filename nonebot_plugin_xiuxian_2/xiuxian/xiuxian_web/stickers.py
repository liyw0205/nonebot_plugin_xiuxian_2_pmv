"""HTTP routes for sticker catalog, installation, and image delivery."""

from __future__ import annotations

import re

from flask import abort, jsonify, request, send_file, session

from ...features.stickers.runtime import sticker_application
from .core import app, logger

_PACK_ID_RE = re.compile(r"^[a-z0-9][a-z0-9_-]{0,31}$")
_STICKER_FILE_RE = re.compile(r"^[A-Za-z0-9._-]+\.webp$")


def _require_admin():
    if "admin_id" not in session:
        return jsonify({"success": False, "error": "未登录"}), 401
    return None


@app.route("/api/messages/stickers", methods=["GET"])
def api_messages_stickers():
    denied = _require_admin()
    if denied:
        return denied
    try:
        refresh = str(request.args.get("refresh") or "").lower() in (
            "1",
            "true",
            "yes",
            "on",
        )
        return jsonify(sticker_application.catalog(force_refresh=refresh))
    except Exception as exc:  # noqa: BLE001
        return jsonify({"success": False, "error": f"获取表情包目录失败: {exc}"})


@app.route("/api/messages/stickers/install", methods=["POST"])
def api_messages_stickers_install():
    denied = _require_admin()
    if denied:
        return denied
    try:
        data = request.get_json(silent=True) or {}
        pack_id = str(data.get("pack_id") or request.args.get("pack_id") or "").strip().lower()
        if not _PACK_ID_RE.fullmatch(pack_id):
            return jsonify({"success": False, "error": "未指定表情包"}), 400
        force = str(data.get("force") or request.args.get("force") or "").lower() in (
            "1",
            "true",
            "yes",
            "on",
        )
        job = sticker_application.start_install(pack_id=pack_id, force=force)
        return jsonify(job), 202 if job.get("status") == "running" else 200
    except Exception as exc:  # noqa: BLE001
        logger.exception("stickers install failed")
        return jsonify({"success": False, "error": f"安装表情包失败: {exc}"})


@app.route("/api/messages/stickers/install/<job_id>", methods=["GET"])
def api_messages_stickers_install_status(job_id: str):
    denied = _require_admin()
    if denied:
        return denied
    job = sticker_application.install_status(job_id)
    if not job:
        return jsonify({"success": False, "error": "安装任务不存在"}), 404
    return jsonify(job)


@app.route("/api/messages/stickers/file/<pack_id>/<path:filename>", methods=["GET"])
def api_messages_stickers_file(pack_id: str, filename: str):
    denied = _require_admin()
    if denied:
        return denied
    if not _PACK_ID_RE.fullmatch(pack_id) or not _STICKER_FILE_RE.fullmatch(filename):
        abort(404)
    path = sticker_application.resolve_file(pack_id, filename)
    if path is None:
        abort(404)
    response = send_file(
        path,
        mimetype="image/webp",
        as_attachment=False,
        download_name=filename,
        conditional=True,
    )
    response.headers["Cache-Control"] = "private, max-age=86400"
    response.headers["Content-Disposition"] = f'inline; filename="{filename}"'
    return response
