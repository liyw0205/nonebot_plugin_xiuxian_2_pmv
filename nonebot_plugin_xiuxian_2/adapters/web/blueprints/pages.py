from __future__ import annotations

import json
import os
import secrets

from flask import Blueprint, redirect, render_template, request, session, url_for

from ..auth import csrf_token
from ._common import guard


PAGE_ENDPOINTS = {
    "config": "/api/v1/config",
    "database": "/api/v1/database",
    "scheduler": "/api/v1/scheduler",
    "backups": "/api/v1/backups",
    "activity": "/api/v1/activity",
    "logs": "/api/v1/logs",
    "commands": "/api/v1/registry",
    "command_registry": "/api/v1/registry",
    "economy_logs": "/api/v1/activity",
    "reward_center": "/api/v1/activity",
    "update": "/api/v1/dashboard",
}


def create_blueprint(*, context=None, permission=None) -> Blueprint:
    blueprint = Blueprint("pages", __name__)
    resolver = permission or (lambda _required: True)

    def admin_ids() -> frozenset[str]:
        settings = getattr(context, "settings", None)
        configured = settings.get("web_admin_ids", ()) if settings is not None else ()
        if isinstance(configured, str):
            configured = (configured,)
        values = [str(item).strip() for item in (configured or ()) if str(item).strip()]
        if not values:
            raw_superusers = os.getenv("SUPERUSERS", "").strip()
            try:
                parsed = json.loads(raw_superusers) if raw_superusers.startswith("[") else raw_superusers.replace(",", " ").split()
            except json.JSONDecodeError:
                parsed = raw_superusers.replace(",", " ").split()
            if isinstance(parsed, (list, tuple, set)):
                values.extend(str(item).strip() for item in parsed if str(item).strip())
        if not values:
            try:
                from nonebot import get_driver

                values.extend(str(item).strip() for item in get_driver().config.superusers)
            except Exception:
                pass
        return frozenset(values)

    @blueprint.route("/", methods=("GET",))
    def home():
        if not resolver("admin"):
            return redirect(url_for("pages.login"))
        return render_template("pages/admin.html", page_name="dashboard", endpoint="/api/v1/dashboard")

    @blueprint.route("/login", methods=("GET", "POST"))
    def login():
        if request.method == "POST":
            expected = session.get("_csrf_token")
            supplied = request.form.get("_csrf_token", "")
            admin_id = str(request.form.get("admin_id") or "").strip()
            if not expected or not supplied or not secrets.compare_digest(str(expected), str(supplied)):
                return render_template("pages/login.html", error="CSRF 校验失败"), 403
            if admin_id not in admin_ids():
                return render_template("pages/login.html", error="无效的管理员 ID"), 401
            session.clear()
            session["role"] = "admin"
            session["admin_id"] = admin_id
            session["_csrf_token"] = csrf_token()
            return redirect(url_for("pages.home"))
        if resolver("admin"):
            return redirect(url_for("pages.home"))
        return render_template("pages/login.html")

    @blueprint.route("/logout", methods=("GET",))
    def logout():
        session.clear()
        return redirect(url_for("pages.login"))

    @blueprint.route("/favicon.ico", methods=("GET",))
    def favicon():
        return "", 204

    @blueprint.route("/robots.txt", methods=("GET",))
    def robots():
        return "User-agent: *\nDisallow: /\n", 200, {"Content-Type": "text/plain; charset=utf-8"}

    for index, (name, endpoint) in enumerate(PAGE_ENDPOINTS.items()):
        @guard("admin", resolver)
        def page(name=name, endpoint=endpoint):
            return render_template("pages/admin.html", page_name=name, endpoint=endpoint)

        blueprint.add_url_rule(f"/pages/{name}", f"page_{index}", page, methods=("GET",))
    return blueprint


__all__ = ["PAGE_ENDPOINTS", "create_blueprint"]
