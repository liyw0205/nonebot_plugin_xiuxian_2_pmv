from __future__ import annotations

from flask import Blueprint, request

from ....core.errors import DomainError
from ..api import api_error, api_success
from ._common import guard


def create_blueprint(*, application, permission) -> Blueprint:
    router = Blueprint("work", __name__)

    @router.post("/api/v1/work/claim")
    @guard("user", permission, write=True)
    def claim():
        payload = request.get_json(silent=True) or {}
        try:
            outcome = application.claim(
                operation_id=request.headers.get("Idempotency-Key") or payload.get("operation_id", ""),
                user_id=payload.get("user_id", ""),
                expected_count=payload.get("expected_count", 0),
                expected_offer=payload.get("expected_offer", {}),
                task_index=payload.get("task_index", 0),
                started_at=payload.get("started_at", ""),
            )
        except DomainError as exc:
            return api_error(exc.code, exc.message, details=exc.details, status=400)
        return api_success(outcome.to_dict(), status=200 if outcome.ok else 409)

    @router.post("/api/v1/work/settle")
    @guard("user", permission, write=True)
    def settle():
        payload = request.get_json(silent=True) or {}
        try:
            outcome = application.settle(
                operation_id=request.headers.get("Idempotency-Key") or payload.get("operation_id", ""),
                user_id=payload.get("user_id", ""),
                expected_work=payload.get("expected_work", {}),
                exp_gain=payload.get("exp_gain", 0),
                item=payload.get("item"),
                max_exp=payload.get("max_exp", 0),
                max_goods_num=payload.get("max_goods_num", 0),
                success_kind=payload.get("success_kind", ""),
                item_msg=payload.get("item_msg", ""),
            )
        except DomainError as exc:
            return api_error(exc.code, exc.message, details=exc.details, status=400)
        return api_success(outcome.to_dict(), status=200 if outcome.ok else 409)

    return router


__all__ = ["create_blueprint"]
