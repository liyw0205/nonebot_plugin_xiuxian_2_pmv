from __future__ import annotations

from typing import Any


def blueprint(application: Any, *, permission: Any):
    from flask import Blueprint, request

    from ...adapters.web.api import api_success
    from ...adapters.web.blueprints._common import guard

    router = Blueprint("info", __name__)

    @router.get("/api/v1/info/users/search")
    @guard("admin", permission)
    def search_users():
        users = application.search_users(request.args.get("query", ""))
        return api_success({"users": users})

    return router


__all__ = ["blueprint"]
