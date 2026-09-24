from __future__ import annotations

from typing import Mapping

from flask import Blueprint, request

from ....core.errors import DomainError
from ..api import api_error, api_success
from ._common import guard


def create_blueprint(
    feature: str,
    application,
    actions: tuple[str, ...],
    permission,
    *,
    action_permissions: Mapping[str, str] | None = None,
) -> Blueprint:
    router = Blueprint(feature, __name__)

    def invoke(action: str):
        payload = request.get_json(silent=True) or {}
        try:
            outcome = getattr(application, action)(
                operation_id=request.headers.get("Idempotency-Key") or payload.get("operation_id", ""),
                user_id=payload.get("user_id", ""),
                **{key: value for key, value in payload.items() if key not in {"operation_id", "user_id"}},
            )
        except DomainError as exc:
            return api_error(exc.code, exc.message, details=exc.details, status=400)
        return api_success(outcome.to_dict(), status=200 if outcome.ok else 409)

    for action in actions:
        required_permission = (action_permissions or {}).get(action, "user")
        router.add_url_rule(
            f"/api/v1/{feature.replace('_', '-')}/{action}",
            endpoint=f"{feature}_{action}",
            view_func=guard(required_permission, permission, write=True)(lambda action=action: invoke(action)),
            methods=["POST"],
        )
    return router


__all__ = ["create_blueprint"]
