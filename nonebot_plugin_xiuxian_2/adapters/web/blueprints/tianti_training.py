from __future__ import annotations

from flask import Blueprint, request

from ....core.errors import DomainError
from ....features.tianti_training.web import parse_datetime
from ..api import api_error, api_success
from ._common import guard


def create_blueprint(*, application, permission) -> Blueprint:
    router = Blueprint("tianti_training", __name__)

    def operation_id(payload):
        return request.headers.get("Idempotency-Key") or payload.get("operation_id", "")

    @router.post("/api/v1/tianti/train")
    @guard("user", permission, write=True)
    def train():
        payload = request.get_json(silent=True) or {}
        try:
            outcome = application.train(
                operation_id=operation_id(payload),
                user_id=payload.get("user_id", ""),
                requested_stone=payload.get("requested_stone", 0),
            )
        except DomainError as exc:
            return api_error(exc.code, exc.message, details=exc.details, status=400)
        return api_success(outcome.to_dict(), status=200 if outcome.ok else 409)

    @router.post("/api/v1/tianti/bath")
    @guard("user", permission, write=True)
    def bath():
        payload = request.get_json(silent=True) or {}
        try:
            outcome = application.apply_bath(
                operation_id=operation_id(payload),
                user_id=payload.get("user_id", ""),
                consume_plan=payload.get("consume_plan", ()),
                effect=payload.get("effect", 0),
                slot_name=payload.get("slot_name", ""),
                started_at=parse_datetime(payload.get("started_at")),
                duration_minutes=payload.get("duration_minutes", 0),
                sect_fairyland_level=payload.get("sect_fairyland_level", 0),
            )
        except (DomainError, ValueError, TypeError) as exc:
            if isinstance(exc, DomainError):
                return api_error(exc.code, exc.message, details=exc.details, status=400)
            return api_error("validation_error", str(exc), status=400)
        return api_success(outcome.to_dict(), status=200 if outcome.ok else 409)

    @router.post("/api/v1/tianti/breakthrough")
    @guard("user", permission, write=True)
    def breakthrough():
        payload = request.get_json(silent=True) or {}
        try:
            outcome = application.breakthrough(
                operation_id=operation_id(payload),
                user_id=payload.get("user_id", ""),
                cultivation_rank=payload.get("cultivation_rank", 0),
                roll_success=payload.get("roll_success", False),
            )
        except DomainError as exc:
            return api_error(exc.code, exc.message, details=exc.details, status=400)
        return api_success(outcome.to_dict(), status=200 if outcome.ok else 409)

    @router.post("/api/v1/tianti/qiaoxue")
    @guard("user", permission, write=True)
    def qiaoxue():
        payload = request.get_json(silent=True) or {}
        try:
            outcome = application.open_qiaoxue(
                operation_id=operation_id(payload),
                user_id=payload.get("user_id", ""),
                roll=payload.get("roll", 0),
            )
        except DomainError as exc:
            return api_error(exc.code, exc.message, details=exc.details, status=400)
        return api_success(outcome.to_dict(), status=200 if outcome.ok else 409)

    return router


__all__ = ["create_blueprint"]
