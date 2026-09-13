from __future__ import annotations

from flask import Blueprint, request

from ....core.errors import DomainError
from ..api import api_error, api_success
from ._common import guard


def create_blueprint(*, application, permission) -> Blueprint:
    router = Blueprint("pet", __name__)

    def invoke(action: str):
        payload = request.get_json(silent=True) or {}
        try:
            outcome = getattr(application, action)(operation_id=request.headers.get("Idempotency-Key") or payload.get("operation_id", ""), **{key: value for key, value in payload.items() if key != "operation_id"})
        except DomainError as exc:
            return api_error(exc.code, exc.message, details=exc.details, status=400)
        return api_success(outcome.to_dict(), status=200 if outcome.ok else 409)

    for path, action in (("travel/claim", "claim_travel"), ("travel/start", "start_travel"), ("feed", "feed"), ("hatch", "hatch")):
        router.add_url_rule(
            f"/api/v1/pet/{path}",
            endpoint=f"pet_{action}",
            view_func=guard("user", permission, write=True)(lambda action=action: invoke(action)),
            methods=["POST"],
        )
    return router


__all__ = ["create_blueprint"]
