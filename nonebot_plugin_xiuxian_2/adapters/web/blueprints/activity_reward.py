import sqlite3
from pathlib import Path

from flask import Blueprint, request

from ..api import api_error, api_success
from ._common import guard
from ....infrastructure.database import DatabaseUnitOfWork


_CLAIM_SCHEMA = {
    "operation_ledger": {
        "operation_id", "action", "request_hash", "status", "result_json",
        "created_at", "updated_at",
    },
    "operation_audit": {
        "audit_id", "operation_id", "action", "status", "before_json",
        "after_json", "consumed_json", "granted_json", "category", "occurred_at",
        "created_at",
    },
    "activity_claim_all_operations": {
        "operation_id", "user_id", "status", "result_json", "created_at", "updated_at",
    },
    "activity_claim_all_steps": {
        "operation_id", "step_name", "ordinal", "status", "attempts", "ok",
        "result_text", "error_text", "updated_at",
    },
}


def _missing_claim_schema(database: Path) -> tuple[str, ...]:
    with DatabaseUnitOfWork(database, read_only=True) as uow:
        missing = []
        for table, required in _CLAIM_SCHEMA.items():
            columns = {
                str(row["name"])
                for row in uow.query_all(f"PRAGMA table_info({table})")
            }
            if not required.issubset(columns):
                missing.append(table)
    return tuple(missing)


def create_blueprint(*, application, permission) -> Blueprint:
    router = Blueprint("activity_reward", __name__)

    @router.post("/api/v1/activity/rewards/claim")
    @guard("admin", permission, write=True)
    def claim_rewards():
        if application is None:
            return api_error("unavailable", "活动领奖服务不可用", status=503)
        payload = request.get_json(silent=True)
        if not isinstance(payload, dict):
            return api_error("validation_error", "请求内容必须是 JSON 对象", status=400)
        raw_user_id = payload.get("user_id")
        if not isinstance(raw_user_id, (str, int)) or isinstance(raw_user_id, bool):
            return api_error("validation_error", "玩家 ID 必须为 1 至 128 个字符", status=400)
        user_id = str(raw_user_id).strip()
        operation_id = str(
            request.headers.get("Idempotency-Key") or payload.get("operation_id") or ""
        ).strip()
        if not user_id or len(user_id) > 128:
            return api_error("validation_error", "玩家 ID 必须为 1 至 128 个字符", status=400)
        if not operation_id or len(operation_id) > 128:
            return api_error("validation_error", "Idempotency-Key 必须为 1 至 128 个字符", status=400)

        database = str(getattr(application, "database", "") or "").strip()
        database_path = Path(database) if database else None
        if database_path is None or not database_path.is_file():
            return api_error(
                "migrations_required", "活动领奖数据库尚未就绪，请先完成启动迁移", status=503
            )
        try:
            missing = _missing_claim_schema(database_path)
        except sqlite3.Error:
            return api_error("storage_unavailable", "活动领奖数据库暂时不可用", status=503)
        if missing:
            return api_error(
                "migrations_required",
                "活动领奖数据结构尚未就绪，请先完成启动迁移",
                details={"missing": list(missing)},
                status=503,
            )
        try:
            outcome = application.claim_all(operation_id=operation_id, user_id=user_id)
        except sqlite3.Error as exc:
            message = str(exc).casefold()
            if "no such table" in message or "no such column" in message:
                return api_error(
                    "migrations_required",
                    "活动领奖数据结构尚未就绪，请先完成启动迁移",
                    status=503,
                )
            return api_error("storage_unavailable", "活动领奖数据库暂时不可用", status=503)
        if not outcome.ok:
            return api_error(
                outcome.code or "rejected",
                outcome.message or "活动奖励领取未完成",
                details=outcome.to_dict(),
                status=409,
            )
        return api_success(outcome.to_dict())

    return router


__all__ = ["create_blueprint"]
