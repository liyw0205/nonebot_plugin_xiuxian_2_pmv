from flask import Blueprint, request

from ..api import api_error, api_success
from ._common import guard
from ....core.errors import DomainError


def create_blueprint(*, application, permission) -> Blueprint:
    router = Blueprint("activity_reward", __name__)

    @router.post("/api/v1/activity/rewards/claim")
    @guard("user", permission, write=True)
    def claim_rewards():
        payload = request.get_json(silent=True) or {}
        try:
            outcome = application.claim_all(
                operation_id=request.headers.get("Idempotency-Key") or payload.get("operation_id", ""),
                user_id=payload.get("user_id", ""),
            )
        except DomainError as exc:
            return api_error(exc.code, exc.message, details=exc.details, status=400)
        return api_success(outcome.to_dict(), status=200 if outcome.ok else 409)

    return router


__all__ = ["create_blueprint"]
