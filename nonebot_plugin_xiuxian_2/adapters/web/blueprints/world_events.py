from __future__ import annotations

from flask import Blueprint, request

from ....core.errors import DomainError
from ..api import api_error, api_success
from ._common import guard


def create_blueprint(*, application, permission) -> Blueprint:
    router = Blueprint("world_events", __name__)

    @router.post("/api/v1/world-events/demon/claim")
    @guard("user", permission, write=True)
    def claim():
        payload = request.get_json(silent=True) or {}
        try:
            outcome = application.claim(
                operation_id=request.headers.get("Idempotency-Key") or payload.get("operation_id", ""),
                event_key=payload.get("event_key", "global"),
                event_id=payload.get("event_id", ""),
                user_id=payload.get("user_id", ""),
                expected_claimed=payload.get("expected_claimed", {}),
                stone=payload.get("stone", 0),
                exp=payload.get("exp", 0),
                items=payload.get("items", ()),
                max_goods_num=payload.get("max_goods_num", 0),
            )
        except DomainError as exc:
            return api_error(exc.code, exc.message, details=exc.details, status=400)
        return api_success(outcome.to_dict(), status=200 if outcome.ok else 409)

    return router


__all__ = ["create_blueprint"]
