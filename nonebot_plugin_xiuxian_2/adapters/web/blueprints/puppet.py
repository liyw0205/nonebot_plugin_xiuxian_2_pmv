from __future__ import annotations

from flask import Blueprint, request

from ....core.errors import DomainError
from ..api import api_error, api_success
from ._common import guard


def create_blueprint(*, application, permission) -> Blueprint:
    router = Blueprint("puppet", __name__)

    @router.post("/api/v1/puppet/purchase")
    @guard("user", permission, write=True)
    def purchase():
        payload = request.get_json(silent=True) or {}
        try:
            outcome = application.purchase(operation_id=request.headers.get("Idempotency-Key") or payload.get("operation_id", ""), user_id=payload.get("user_id", ""), stone_cost=payload.get("stone_cost", 0))
        except DomainError as exc:
            return api_error(exc.code, exc.message, details=exc.details, status=400)
        return api_success(outcome.to_dict(), status=200 if outcome.ok else 409)

    @router.post("/api/v1/puppet/upgrade")
    @guard("user", permission, write=True)
    def upgrade():
        payload = request.get_json(silent=True) or {}
        try:
            outcome = application.upgrade(operation_id=request.headers.get("Idempotency-Key") or payload.get("operation_id", ""), user_id=payload.get("user_id", ""), upgrade_costs=payload.get("upgrade_costs", {}), max_level=payload.get("max_level", 0))
        except DomainError as exc:
            return api_error(exc.code, exc.message, details=exc.details, status=400)
        return api_success(outcome.to_dict(), status=200 if outcome.ok else 409)

    return router


__all__ = ["create_blueprint"]
