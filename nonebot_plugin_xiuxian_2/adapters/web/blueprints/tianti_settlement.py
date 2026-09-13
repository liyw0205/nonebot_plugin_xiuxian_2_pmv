from flask import Blueprint, request

from ....core.errors import DomainError
from ....features.tianti_settlement.web import parse_datetime
from ..api import api_error, api_success
from ._common import guard


def create_blueprint(*, application, permission) -> Blueprint:
    router = Blueprint("tianti_settlement", __name__)

    @router.post("/api/v1/tianti/settle")
    @guard("user", permission, write=True)
    def settle_tianti():
        payload = request.get_json(silent=True) or {}
        try:
            settled_at = parse_datetime(payload.get("settled_at"))
            outcome = application.settle(
                operation_id=request.headers.get("Idempotency-Key") or payload.get("operation_id", ""),
                user_id=payload.get("user_id", ""),
                settled_at=settled_at,
                sect_fairyland_level=payload.get("sect_fairyland_level", 0),
            )
        except (DomainError, ValueError) as exc:
            if isinstance(exc, DomainError):
                return api_error(exc.code, exc.message, details=exc.details, status=400)
            return api_error("validation_error", str(exc), status=400)
        return api_success(outcome.to_dict(), status=200 if outcome.ok else 409)

    return router


__all__ = ["create_blueprint"]
