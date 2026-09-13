from __future__ import annotations

import secrets
from functools import wraps
from typing import Any, Callable

from ...core.errors import DomainError
from ...infrastructure.ids import UUIDGenerator


def request_id(ids: Any | None = None) -> str:
    ids = ids or UUIDGenerator()
    try:
        from flask import g, request

        current = getattr(g, "request_id", "")
        if current:
            return current
        current = request.headers.get("X-Request-ID") or ids.new_id()
        # Keep the value bounded and header-safe; external IDs are only
        # correlation metadata and must never become log injection vectors.
        current = "".join(char for char in current if char.isalnum() or char in "-_.")[:128] or ids.new_id()
        g.request_id = current
        return current
    except RuntimeError:
        return ids.new_id()


def api_success(data: Any = None, *, request_id_value: str | None = None, status: int = 200):
    from flask import jsonify

    return jsonify({"ok": True, "data": data, "request_id": request_id_value or request_id()}), status


def api_error(code: str, message: str, *, details: Any = None, status: int = 400, request_id_value: str | None = None):
    from flask import jsonify

    return jsonify({"ok": False, "error": {"code": code, "message": message, "details": details if details is not None else {}}, "request_id": request_id_value or request_id()}), status


def _require_csrf():
    from flask import request, session

    expected = session.get("_csrf_token")
    supplied = request.headers.get("X-CSRF-Token") or request.form.get("_csrf_token")
    if expected and supplied and secrets.compare_digest(str(expected), str(supplied)):
        return None
    return api_error("csrf_failed", "CSRF 校验失败", status=403)


def create_api_blueprint(application: Any, *, name: str = "refactored_api", url_prefix: str = "/api/v1", permission: Callable[[str], bool] | None = None, ids: Any | None = None):
    from flask import Blueprint, request

    blueprint = Blueprint(name, __name__, url_prefix=url_prefix)
    ids = ids or UUIDGenerator()

    def endpoint_permission(required: str):
        def decorator(function):
            @wraps(function)
            def wrapped(*args, **kwargs):
                if permission is not None and not permission(required):
                    return api_error("forbidden", "权限不足", status=403)
                if request.method in {"POST", "PUT", "PATCH", "DELETE"}:
                    failed = _require_csrf()
                    if failed is not None:
                        return failed
                try:
                    return function(*args, **kwargs)
                except DomainError as exc:
                    status = 403 if exc.code == "forbidden" else 404 if exc.code == "not_found" else 409 if exc.code == "conflict" else 400
                    return api_error(exc.code, exc.message, details=exc.details, status=status)
                except Exception:
                    return api_error("internal_error", "服务暂时不可用", status=500)

            return wrapped

        return decorator

    @blueprint.route("/daily-fortune", methods=["GET", "POST"])
    @endpoint_permission("user")
    def daily_fortune():
        payload = request.get_json(silent=True) or {}
        user_id = str(request.args.get("user_id") or payload.get("user_id") or "")
        operation_id = str(request.headers.get("Idempotency-Key") or request.args.get("operation_id") or payload.get("operation_id") or ids.new_id())
        date = request.args.get("date") or payload.get("date")
        result = application.claim(user_id=user_id, operation_id=operation_id, date=date)
        if not result.ok:
            return api_error(
                result.code or "rejected",
                result.message or "请求被拒绝",
                details=result.to_dict(),
                status=409,
            )
        return api_success(result.to_dict())

    return blueprint


def create_sign_in_blueprint(application: Any, *, permission: Callable[[str], bool] | None = None, ids: Any | None = None):
    """Build the sign-in endpoint while keeping Flask imports in the adapter."""
    from flask import Blueprint, request

    blueprint = Blueprint("sign_in", __name__)
    ids = ids or UUIDGenerator()

    @blueprint.post("/api/v1/sign-in")
    def claim():
        if permission is not None and not permission("user"):
            return api_error("forbidden", "权限不足", status=403)
        payload = request.get_json(silent=True) or {}
        if not isinstance(payload, dict):
            return api_error("validation_error", "请求体必须是 JSON 对象", status=400)
        user_id = str(payload.get("user_id") or request.args.get("user_id") or "")
        operation_id = str(
            request.headers.get("Idempotency-Key")
            or payload.get("operation_id")
            or ids.new_id()
        )
        try:
            failed = _require_csrf()
            if failed is not None:
                return failed
            limits = {}
            if "lower_limit" in payload:
                limits["lower_limit"] = int(payload["lower_limit"])
            if "upper_limit" in payload:
                limits["upper_limit"] = int(payload["upper_limit"])
            result = application.claim(user_id=user_id, operation_id=operation_id, **limits)
        except (TypeError, ValueError) as exc:
            return api_error("validation_error", str(exc), status=400)
        except DomainError as exc:
            status = 403 if exc.code == "forbidden" else 404 if exc.code == "not_found" else 409 if exc.code == "conflict" else 400
            return api_error(exc.code, exc.message, details=exc.details, status=status)
        return api_success(result.to_dict(), status=200 if result.ok else 409)

    return blueprint


def create_stone_gift_blueprint(application: Any, *, permission: Callable[[str], bool] | None = None, ids: Any | None = None):
    """Build the idempotent spirit-stone transfer endpoint in the Web adapter."""
    from flask import Blueprint, request

    blueprint = Blueprint("stone_gift", __name__)
    ids = ids or UUIDGenerator()

    @blueprint.post("/api/v1/stone-gift")
    def transfer():
        if permission is not None and not permission("user"):
            return api_error("forbidden", "权限不足", status=403)
        payload = request.get_json(silent=True) or {}
        if not isinstance(payload, dict):
            return api_error("validation_error", "请求体必须是 JSON 对象", status=400)
        operation_id = str(
            request.headers.get("Idempotency-Key")
            or payload.get("operation_id")
            or ids.new_id()
        )
        try:
            failed = _require_csrf()
            if failed is not None:
                return failed
            required = ("sender_id", "recipient_id", "gross_amount")
            missing = [name for name in required if payload.get(name) in (None, "")]
            if missing:
                return api_error("validation_error", "缺少必要字段", details={"fields": missing}, status=400)
            kwargs = {
                "operation_id": operation_id,
                "sender_id": str(payload["sender_id"]),
                "recipient_id": str(payload["recipient_id"]),
                "gross_amount": int(payload["gross_amount"]),
            }
            if "fee_rate" in payload:
                kwargs["fee_rate"] = float(payload["fee_rate"])
            for field in ("transfer_date", "send_limit", "receive_limit"):
                if field in payload:
                    kwargs[field] = payload[field] if field == "transfer_date" else int(payload[field])
            result = application.transfer(**kwargs)
        except (TypeError, ValueError) as exc:
            return api_error("validation_error", str(exc), status=400)
        except DomainError as exc:
            status = 403 if exc.code == "forbidden" else 404 if exc.code == "not_found" else 409 if exc.code == "conflict" else 400
            return api_error(exc.code, exc.message, details=exc.details, status=status)
        return api_success(result.to_dict(), status=200 if result.ok else 409)

    return blueprint


__all__ = [
    "api_error",
    "api_success",
    "create_api_blueprint",
    "create_sign_in_blueprint",
    "create_stone_gift_blueprint",
]
