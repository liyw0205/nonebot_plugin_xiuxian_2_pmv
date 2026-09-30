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
        ("breakthrough", "tribulation", "rename", "stone_contest", "stone_robbery", "sign"),
        permission,
    )

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
