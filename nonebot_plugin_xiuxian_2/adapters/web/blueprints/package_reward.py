from __future__ import annotations

from typing import Any

from flask import Blueprint, request

from ....core.errors import DomainError
from ....features.package_reward.domain import PackageReward
from ..api import api_error, api_success
from ._common import guard


def create_blueprint(*, application: Any, permission=None) -> Blueprint:
    router = Blueprint("package_reward", __name__)

    @router.post("/api/v1/package-reward/open")
    @guard("user", permission, write=True)
    def open_package():
        raw_payload = request.get_json(silent=True) or {}
        payload: dict[str, Any] = raw_payload if isinstance(raw_payload, dict) else {}
        if not isinstance(raw_payload, dict):
            return api_error("validation_error", "请求体必须是 JSON 对象", status=400)
        operation_id = str(request.headers.get("Idempotency-Key") or payload.get("operation_id") or "")
        try:
            def int_field(name: str) -> int:
                value = payload.get(name)
                if value in (None, ""):
                    raise ValueError(f"missing field: {name}")
                return int(str(value))

            rewards = tuple(
                PackageReward(
                    None if item.get("item_id") is None else int(item["item_id"]),
                    str(item["name"]),
                    None if item.get("item_type") is None else str(item["item_type"]),
                    int(item["quantity"]),
                )
                for item in payload.get("rewards", [])
            )
            outcome = application.open_package(
                operation_id=operation_id,
                user_id=str(payload.get("user_id", "")),
                package_id=int_field("package_id"),
                quantity=int_field("quantity"),
                rewards=rewards,
                max_goods_num=int_field("max_goods_num"),
            )
        except (TypeError, ValueError, KeyError) as exc:
            return api_error("validation_error", str(exc), status=400)
        except DomainError as exc:
            return api_error(exc.code, exc.message, details=exc.details, status=400)
        status = 200 if outcome.ok else 409
        return api_success(outcome.to_dict(), status=status)

    return router


__all__ = ["create_blueprint"]
