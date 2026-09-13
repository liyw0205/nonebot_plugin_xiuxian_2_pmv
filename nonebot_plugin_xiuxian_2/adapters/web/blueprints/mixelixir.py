from __future__ import annotations

from flask import Blueprint, request

from ....core.errors import DomainError
from ..api import api_error, api_success
from ._common import guard


def create_blueprint(*, application, permission) -> Blueprint:
    router = Blueprint("mixelixir", __name__)

    @router.post("/api/v1/mixelixir/harvest")
    @guard("user", permission, write=True)
    def harvest():
        payload = request.get_json(silent=True) or {}
        try:
            outcome = application.harvest(
                operation_id=request.headers.get("Idempotency-Key") or payload.get("operation_id", ""), user_id=payload.get("user_id", ""),
                expected_last_time=payload.get("expected_last_time", ""), harvested_at=payload.get("harvested_at", ""), rewards=payload.get("rewards", ()), max_goods_num=payload.get("max_goods_num", 0),
            )
        except DomainError as exc:
            return api_error(exc.code, exc.message, details=exc.details, status=400)
        return api_success(outcome.to_dict(), status=200 if outcome.ok else 409)

    @router.post("/api/v1/mixelixir/settle")
    @guard("user", permission, write=True)
    def settle():
        payload = request.get_json(silent=True) or {}
        try:
            outcome = application.settle(
                operation_id=request.headers.get("Idempotency-Key") or payload.get("operation_id", ""), user_id=payload.get("user_id", ""),
                materials=payload.get("materials", {}), reward_id=payload.get("reward_id", 0), reward_name=payload.get("reward_name", ""), reward_quantity=payload.get("reward_quantity", 0), max_goods_num=payload.get("max_goods_num", 0),
            )
        except DomainError as exc:
            return api_error(exc.code, exc.message, details=exc.details, status=400)
        return api_success(outcome.to_dict(), status=200 if outcome.ok else 409)

    return router


__all__ = ["create_blueprint"]
