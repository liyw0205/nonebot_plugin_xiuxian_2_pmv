from flask import Blueprint, request

from ....core.errors import DomainError
from ..api import api_error, api_success
from ._common import guard


def create_blueprint(*, application, permission) -> Blueprint:
    router = Blueprint("sect_fairyland", __name__)

    @router.post("/api/v1/sect/fairyland/claim")
    @guard("user", permission, write=True)
    def claim():
        payload = request.get_json(silent=True) or {}
        try:
            outcome = application.claim(
                operation_id=request.headers.get("Idempotency-Key") or payload.get("operation_id", ""),
                user_id=payload.get("user_id", ""),
                sect_id=payload.get("sect_id", ""),
                day=payload.get("day", ""),
                level=payload.get("level", 0),
                minutes=payload.get("minutes", 0),
            )
        except DomainError as exc:
            return api_error(exc.code, exc.message, details=exc.details, status=400)
        return api_success(outcome.to_dict(), status=200 if outcome.ok else 409)

    return router


__all__ = ["create_blueprint"]
