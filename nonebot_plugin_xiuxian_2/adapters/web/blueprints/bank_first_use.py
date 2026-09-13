from __future__ import annotations

from typing import Any

from flask import Blueprint, request

from ....core.errors import DomainError
from ..api import api_error, api_success
from ._common import guard


def create_first_use_blueprint(*, application: Any, permission) -> Blueprint:
    router = Blueprint("bank_first_use", __name__)

    @router.post("/api/v1/bank/v2/deposit")
    @guard("user", permission, write=True)
    def deposit():
        payload = request.get_json(silent=True) or {}
        if not isinstance(payload, dict):
            return api_error("validation_error", "请求体必须是 JSON 对象", status=400)
        operation_id = str(request.headers.get("Idempotency-Key") or payload.get("operation_id") or "")
        try:
            def int_field(name: str, default: int | None = None) -> int:
                value = payload.get(name, default)
                if value in (None, ""):
                    raise ValueError(f"missing field: {name}")
                return int(str(value))

            outcome = application.deposit(
                operation_id=operation_id,
                user_id=str(payload.get("user_id", "")),
                amount=int_field("amount"),
                interest=int_field("interest", 0),
                limit=int_field("limit"),
                bank_level=str(payload.get("bank_level", "1")),
                settled_at=str(payload.get("settled_at", "")),
            )
        except (TypeError, ValueError) as exc:
            return api_error("validation_error", str(exc), status=400)
        except DomainError as exc:
            return api_error(exc.code, exc.message, details=exc.details, status=400)
        return api_success(outcome, status=200 if outcome.get("status") in {"applied", "duplicate"} else 409)

    return router


def create_upgrade_blueprint(*, application: Any, permission) -> Blueprint:
    router = Blueprint("bank_first_use_upgrade", __name__)

    @router.post("/api/v1/bank/v2/upgrade")
    @guard("user", permission, write=True)
    def upgrade():
        payload = request.get_json(silent=True) or {}
        if not isinstance(payload, dict):
            return api_error("validation_error", "请求体必须是 JSON 对象", status=400)
        operation_id = str(request.headers.get("Idempotency-Key") or payload.get("operation_id") or "")
        try:
            outcome = application.upgrade(
                operation_id=operation_id,
                user_id=str(payload.get("user_id", "")),
                expected_level=str(payload.get("expected_level", "")),
                next_level=str(payload.get("next_level", "")),
                cost=int(str(payload.get("cost"))),
                settled_at=str(payload.get("settled_at", "")),
            )
        except (TypeError, ValueError) as exc:
            return api_error("validation_error", str(exc), status=400)
        status = 200 if outcome.get("status") in {"applied", "duplicate"} else 409
        return api_success(outcome, status=status)

    return router


def create_interest_blueprint(*, application: Any, permission) -> Blueprint:
    router = Blueprint("bank_first_use_interest", __name__)

    @router.post("/api/v1/bank/v2/interest")
    @guard("user", permission, write=True)
    def interest():
        payload = request.get_json(silent=True) or {}
        if not isinstance(payload, dict):
            return api_error("validation_error", "请求体必须是 JSON 对象", status=400)
        operation_id = str(request.headers.get("Idempotency-Key") or payload.get("operation_id") or "")
        try:
            value = payload.get("interest")
            if value in (None, ""):
                raise ValueError("missing field: interest")
            outcome = application.settle_interest(operation_id=operation_id, user_id=str(payload.get("user_id", "")), interest=int(str(value)), bank_level=str(payload.get("bank_level", "1")), settled_at=str(payload.get("settled_at", "")))
        except (TypeError, ValueError) as exc:
            return api_error("validation_error", str(exc), status=400)
        status = 200 if outcome.get("status") in {"applied", "duplicate"} else 409
        return api_success(outcome, status=status)

    return router


def create_info_blueprint(*, application: Any, permission) -> Blueprint:
    router = Blueprint("bank_first_use_info", __name__)

    @router.get("/api/v1/bank/v2/info")
    @guard("user", permission)
    def info():
        user_id = str(request.args.get("user_id", "")).strip()
        try:
            outcome = application.get_info(user_id=user_id)
        except (TypeError, ValueError) as exc:
            return api_error("validation_error", str(exc), status=400)
        return api_success(outcome, status=200 if outcome.get("status") == "ok" else 404)

    return router


__all__ = ["create_first_use_blueprint", "create_info_blueprint", "create_interest_blueprint", "create_upgrade_blueprint"]
