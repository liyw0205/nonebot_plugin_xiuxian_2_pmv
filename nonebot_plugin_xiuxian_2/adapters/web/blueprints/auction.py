from __future__ import annotations

import sqlite3

from flask import Blueprint, request

from ..api import api_error, api_success
from ._common import guard
from ....core.errors import DomainError
from ....infrastructure.clock import SystemClock
from ....infrastructure.ids import UUIDGenerator


def create_blueprint(*, application, settlement=None, permission, clock=None, ids=None) -> Blueprint:
    router = Blueprint("auction", __name__)
    clock = clock or SystemClock()
    ids = ids or UUIDGenerator()

    @router.post("/api/v1/auction/bids")
    @guard("user", permission, write=True)
    def place_bid():
        payload = request.get_json(silent=True) or {}
        try:
            outcome = application.place_bid(
                operation_id=request.headers.get("Idempotency-Key") or payload.get("operation_id") or ids.new_id(),
                auction_id=payload.get("auction_id", ""), bidder_id=payload.get("bidder_id", ""),
                bid_price=payload.get("bid_price", 0), expected_price=payload.get("expected_price", 0),
                expected_bids=payload.get("expected_bids", {}), bid_time=payload.get("bid_time", 0),
            )
        except sqlite3.OperationalError as exc:
            if "no such table" not in str(exc):
                raise
            return api_error("migrations_required", "数据库尚未完成迁移", status=503)
        except DomainError as exc:
            return api_error(exc.code, exc.message, details=exc.details, status=400)
        return api_success(outcome.to_dict(), status=200 if outcome.ok else 409)

    @router.post("/api/v1/auction/settle")
    @guard("admin", permission, write=True)
    def settle_auction():
        payload = request.get_json(silent=True) or {}
        try:
            outcome = (settlement or application).settle_active(
                operation_id=request.headers.get("Idempotency-Key") or payload.get("operation_id") or ids.new_id(),
                end_time=payload.get("end_time", clock.now().timestamp()),
                fee_rate=payload.get("fee_rate", 0.1),
                item_types=payload.get("item_types", {}),
            )
        except sqlite3.OperationalError as exc:
            if "no such table" not in str(exc):
                raise
            return api_error("migrations_required", "数据库尚未完成迁移", status=503)
        except DomainError as exc:
            return api_error(exc.code, exc.message, details=exc.details, status=400)
        return api_success(outcome.to_dict(), status=200 if outcome.ok else 409)

    return router


__all__ = ["create_blueprint"]
