from __future__ import annotations

from flask import Blueprint, request

from ....core.errors import DomainError
from ..api import api_error, api_success
from ._common import guard


def create_blueprint(*, application, permission) -> Blueprint:
    router = Blueprint("boss", __name__)

    @router.post("/api/v1/boss/purchase")
    @guard("user", permission, write=True)
    def purchase():
        payload = request.get_json(silent=True) or {}
        values = {key: value for key, value in payload.items() if key != "operation_id"}
        try:
            outcome = application.purchase(
                operation_id=request.headers.get("Idempotency-Key") or payload.get("operation_id", ""),
                **values,
            )
        except DomainError as exc:
            return api_error(exc.code, exc.message, details=exc.details, status=400)
        return api_success(outcome.to_dict(), status=200 if outcome.ok else 409)

    @router.post("/api/v1/boss/settle")
    @guard("user", permission, write=True)
    def settle():
        payload = request.get_json(silent=True) or {}
        values = {key: value for key, value in payload.items() if key != "operation_id"}
        try:
            outcome = application.settle(
                operation_id=request.headers.get("Idempotency-Key") or payload.get("operation_id", ""),
                **values,
            )
        except DomainError as exc:
            return api_error(exc.code, exc.message, details=exc.details, status=400)
        return api_success(outcome.to_dict(), status=200 if outcome.ok else 409)

    return router


__all__ = ["create_blueprint"]
