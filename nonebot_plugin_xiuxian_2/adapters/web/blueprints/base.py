from __future__ import annotations

from dataclasses import asdict

from flask import Blueprint, request

from ..api import api_success
from ._common import guard
from .legacy_feature import create_blueprint as create_legacy_blueprint


def create_blueprint(*, application, permission) -> Blueprint:
    router = create_legacy_blueprint(
        "base",
        application,
        ("breakthrough", "tribulation", "rename", "stone_robbery", "sign"),
        permission,
    )

    @router.post("/api/v1/base/stone_contest")
    @guard("user", permission, write=True)
    def stone_contest():
        payload = request.get_json(silent=True) or {}
        if not isinstance(payload, dict):
            return api_error("validation_error", "请求体必须是 JSON 对象", status=400)
        payer_id = str(payload.get("payer_id") or payload.get("user_id") or "")
        receiver_id = str(payload.get("receiver_id") or payload.get("recipient_id") or "")
        requested_amount = payload.get("requested_amount", payload.get("amount", 0))
        result = application.stone_contest(
            operation_id=request.headers.get("Idempotency-Key") or payload.get("operation_id", ""),
            user_id=payer_id,
            payer_id=payer_id,
            receiver_id=receiver_id,
            requested_amount=requested_amount,
        )
        return api_success(result.to_dict(), status=200 if result.succeeded else 409)

    @router.post("/api/v1/base/xiangyuan/create")
    @guard("user", permission, write=True)
    def xiangyuan_create():
        payload = request.get_json(silent=True) or {}
        result = application.xiangyuan_create(
            operation_id=request.headers.get("Idempotency-Key") or payload.get("operation_id", ""),
            user_id=payload.get("user_id", ""),
            group_id=payload.get("group_id", ""),
            giver_name=payload.get("giver_name", ""),
            stone=payload.get("stone", 0),
            items=payload.get("items", ()),
            receiver_count=payload.get("receiver_count", 0),
            send_limit=payload.get("send_limit", 3),
            legacy_data=payload.get("legacy_data"),
        )
        return api_success(asdict(result), status=200 if result.succeeded else 409)

    @router.post("/api/v1/base/xiangyuan/claim")
    @guard("user", permission, write=True)
    def xiangyuan_claim():
        payload = request.get_json(silent=True) or {}
        result = application.xiangyuan_claim(
            operation_id=request.headers.get("Idempotency-Key") or payload.get("operation_id", ""),
            user_id=payload.get("user_id", ""),
            group_id=payload.get("group_id", ""),
            gift_id=payload.get("gift_id", 0),
            stone_reward=payload.get("stone_reward", 0),
            item_ids=payload.get("item_ids", ()),
            receive_limit=payload.get("receive_limit", 3),
            max_goods_num=payload.get("max_goods_num", 0),
            legacy_data=payload.get("legacy_data"),
        )
        return api_success(asdict(result), status=200 if result.succeeded else 409)

    @router.get("/api/v1/base/xiangyuan/group")
    @guard("user", permission)
    def xiangyuan_group():
        return api_success(application.xiangyuan_group(group_id=request.args.get("group_id", "")))

    return router


__all__ = ["create_blueprint"]
