"""Flask adapter for the player-info read model."""

from __future__ import annotations

from flask import Blueprint, request

from ..api import api_success
from ._common import guard


def create_blueprint(*, application, permission) -> Blueprint:
    router = Blueprint("info", __name__)

    @router.get("/api/v1/info/users/search")
    @guard("admin", permission)
    def search_users():
        users = application.search_users(request.args.get("query", ""))
        return api_success({"users": users})

    return router


__all__ = ["create_blueprint"]
