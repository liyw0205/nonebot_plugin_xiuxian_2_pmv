from flask import Blueprint, request

from ....core.errors import DomainError
from ..api import api_error, api_success
from ._common import guard


def create_blueprint(*, application, permission) -> Blueprint:
    router = Blueprint("admin_asset", __name__)

    @router.post("/api/v1/admin/assets/stone")
    @guard("admin", permission, write=True)
    def adjust_stone():
        payload = request.get_json(silent=True) or {}
        try:
            outcome = application.adjust_stone(
                operation_id=request.headers.get("Idempotency-Key") or payload.get("operation_id", ""),
                operator_id=payload.get("operator_id", ""),
                user_id=payload.get("user_id", ""),
                expected_stone=payload.get("expected_stone", 0),
                requested_delta=payload.get("requested_delta", 0),
                target_name=payload.get("target_name", ""),
            )
        except DomainError as exc:
            return api_error(exc.code, exc.message, details=exc.details, status=400)
        return api_success(outcome.to_dict(), status=200 if outcome.ok else 409)

    @router.post("/api/v1/admin/assets/item")
    @guard("admin", permission, write=True)
    def grant_item():
        payload = request.get_json(silent=True) or {}
        try:
            outcome = application.grant_item(
                operation_id=request.headers.get("Idempotency-Key") or payload.get("operation_id", ""),
                operator_id=payload.get("operator_id", ""),
                user_id=payload.get("user_id", ""),
                item_id=payload.get("item_id", 0),
                item_name=payload.get("item_name", ""),
                item_type=payload.get("item_type", ""),
                quantity=payload.get("quantity", 0),
                expected_quantity=payload.get("expected_quantity", 0),
                max_goods_num=payload.get("max_goods_num", 0),
                target_name=payload.get("target_name", ""),
            )
        except DomainError as exc:
            return api_error(exc.code, exc.message, details=exc.details, status=400)
        return api_success(outcome.to_dict(), status=200 if outcome.ok else 409)

    return router


__all__ = ["create_blueprint"]
