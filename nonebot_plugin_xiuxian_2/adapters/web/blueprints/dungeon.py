from __future__ import annotations

import sqlite3

from flask import Blueprint, request

from ....core.errors import DomainError
from ..api import api_error, api_success
from ._common import guard


def create_blueprint(*, application, permission) -> Blueprint:
    router = Blueprint("dungeon", __name__)

    @router.post("/api/v1/dungeon/purchase")
    @guard("user", permission, write=True)
    def purchase():
        payload = request.get_json(silent=True) or {}
        try:
            outcome = application.purchase(
                operation_id=request.headers.get("Idempotency-Key") or payload.get("operation_id", ""),
                **{key: value for key, value in payload.items() if key != "operation_id"},
            )
        except sqlite3.OperationalError as exc:
            if "no such table" not in str(exc):
                raise
            return api_error("migrations_required", "数据库尚未完成迁移", status=503)
        except DomainError as exc:
            return api_error(exc.code, exc.message, details=exc.details, status=400)
        return api_success(outcome.to_dict(), status=200 if outcome.ok else 409)

    def explore_action(action: str):
        payload = request.get_json(silent=True) or {}
        operation_id = request.headers.get("Idempotency-Key") or payload.get("operation_id", "")
        try:
            if action == "replay":
                result = application.replay(operation_id=operation_id, user_id=payload.get("user_id", ""))
            elif action == "prepare":
                result = application.prepare(operation_id=operation_id, user_id=payload.get("user_id", ""), plan=payload.get("plan", {}))
            else:
                result = application.settle(operation_id=operation_id, user_id=payload.get("user_id", ""), max_goods_num=payload.get("max_goods_num", 0))
        except sqlite3.OperationalError as exc:
            if "no such table" not in str(exc):
                raise
            return api_error("migrations_required", "数据库尚未完成迁移", status=503)
        except DomainError as exc:
            return api_error(exc.code, exc.message, details=exc.details, status=400)
        data = result if isinstance(result, dict) else getattr(result, "__dict__", {})
        status = str(data.get("status", ""))
        return api_success(data, status=200 if status in {"applied", "duplicate", "completed", "prepared"} else 409)

    for path, action in (("replay", "replay"), ("prepare", "prepare"), ("settle", "settle")):
        router.add_url_rule(
            f"/api/v1/dungeon/explore/{path}",
            endpoint=f"explore_{action}",
            view_func=guard("user", permission, write=True)(lambda action=action: explore_action(action)),
            methods=["POST"],
        )
    return router


__all__ = ["create_blueprint"]
