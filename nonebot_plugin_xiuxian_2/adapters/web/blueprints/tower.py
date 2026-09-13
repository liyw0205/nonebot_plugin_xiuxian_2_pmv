from __future__ import annotations

from flask import Blueprint, request

from ....core.errors import DomainError
from ..api import api_error, api_success
from ._common import guard


def create_blueprint(*, application, permission) -> Blueprint:
    router = Blueprint("tower", __name__)

    @router.post("/api/v1/tower/purchase")
    @guard("user", permission, write=True)
    def purchase():
        payload = request.get_json(silent=True) or {}
        try:
            outcome = application.purchase(
                operation_id=request.headers.get("Idempotency-Key") or payload.get("operation_id", ""),
                user_id=payload.get("user_id", ""),
                item_id=payload.get("item_id", 0), item_name=payload.get("item_name", ""), item_type=payload.get("item_type", ""),
                quantity=payload.get("quantity", 0), unit_cost=payload.get("unit_cost", 0), weekly_limit=payload.get("weekly_limit", 0),
                expected_score=payload.get("expected_score", 0), expected_weekly_purchases=payload.get("expected_weekly_purchases", {}),
                max_goods_num=payload.get("max_goods_num", 0), bind_flag=payload.get("bind_flag", 1), today=payload.get("today"),
            )
        except DomainError as exc:
            return api_error(exc.code, exc.message, details=exc.details, status=400)
        return api_success(outcome.to_dict(), status=200 if outcome.ok else 409)

    @router.post("/api/v1/tower/settle")
    @guard("user", permission, write=True)
    def settle():
        payload = request.get_json(silent=True) or {}
        try:
            outcome = application.settle(
                operation_id=request.headers.get("Idempotency-Key") or payload.get("operation_id", ""),
                user_id=payload.get("user_id", ""), expected_tower=payload.get("expected_tower", {}),
                floor=payload.get("floor", 0), score=payload.get("score", 0), stone=payload.get("stone", 0), exp=payload.get("exp", 0),
                items=payload.get("items", ()), max_goods_num=payload.get("max_goods_num", 0), expected_player=payload.get("expected_player"),
                final_hp=payload.get("final_hp"), final_mp=payload.get("final_mp"), stamina_cost=payload.get("stamina_cost", 0),
                challenge_succeeded=payload.get("challenge_succeeded", True),
            )
        except DomainError as exc:
            return api_error(exc.code, exc.message, details=exc.details, status=400)
        return api_success(outcome.to_dict(), status=200 if outcome.ok else 409)

    return router


__all__ = ["create_blueprint"]
