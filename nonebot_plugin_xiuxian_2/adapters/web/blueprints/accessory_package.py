from __future__ import annotations

from flask import Blueprint, request

from ..api import api_error, api_success
from ._common import guard
from ....core.errors import DomainError
from ....features.package_reward.domain import PackageReward


def create_blueprint(*, application, permission) -> Blueprint:
    router = Blueprint("accessory_package", __name__)

    @router.post("/api/v1/accessory-package")
    @guard("user", permission, write=True)
    def open_package():
        payload = request.get_json(silent=True) or {}
        try:
            rewards = tuple(PackageReward(**item) if isinstance(item, dict) else item for item in payload.get("rewards", ()))
            result = application.open_package(
                operation_id=request.headers.get("Idempotency-Key") or payload.get("operation_id", ""),
                user_id=payload.get("user_id", ""),
                package_id=payload.get("package_id", 0),
                quantity=payload.get("quantity", 0),
                rewards=rewards,
                accessories=payload.get("accessories", ()),
                max_goods_num=payload.get("max_goods_num", 0),
                accessory_limit=payload.get("accessory_limit", 0),
            )
        except DomainError as exc:
            return api_error(exc.code, exc.message, details=exc.details, status=400)
        return api_success(result.to_dict(), status=200 if result.status not in {"failed"} else 500)

    return router


__all__ = ["create_blueprint"]
