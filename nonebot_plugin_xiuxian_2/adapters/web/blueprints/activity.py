import sqlite3
from pathlib import Path

from flask import Blueprint, request

from ..api import api_error, api_success
from ._common import guard
from ....infrastructure.database import DatabaseUnitOfWork


_SIGN_IN_SCHEMA = {
    "operation_ledger": {
        "operation_id", "action", "request_hash", "status", "result_json",
        "created_at", "updated_at",
    },
    "operation_audit": {
        "audit_id", "operation_id", "action", "status", "before_json",
        "after_json", "consumed_json", "granted_json", "category", "occurred_at",
        "created_at",
    },
    "activity_sign_settlement_operations": {
        "operation_id", "payload", "sign_days", "total_sign_days",
    },
    "activity_event_operations": {"event_id", "payload_hash", "created_at"},
    "activity_user": {
        "user_id", "sign_days", "last_sign_date", "total_sign_days",
        "create_time", "update_time",
    },
    "activity_sign_log": {
        "user_id", "sign_date", "day_index", "reward", "milestone_reward",
        "reward_status", "reward_message", "create_time", "finish_time",
    },
    "user_xiuxian": {"user_id", "stone"},
    "back": {
        "user_id", "goods_id", "goods_name", "goods_type", "goods_num",
        "create_time", "update_time", "bind_num",
    },
}
_SIGN_IN_TABLES = frozenset(
    {
        "activity_collect_inventory",
        "activity_collect_claim",
        "activity_collect_drop_log",
        "activity_collect_pity_state",
        "activity_point_balance",
        "activity_point_event_log",
        "activity_point_purchase",
        "activity_task_progress",
        "activity_task_claim_log",
        "activity_pass_balance",
        "activity_pass_event_log",
        "activity_pass_reward_claim",
        "activity_item_inventory",
        "activity_boss_state",
        "activity_boss_damage",
        "activity_boss_fight_log",
        "activity_boss_milestone",
        "activity_boss_milestone_claim",
        "activity_boss_rank_claim",
    }
    | set(_SIGN_IN_SCHEMA)
)


def _missing_sign_schema(database: Path) -> tuple[str, ...]:
    with DatabaseUnitOfWork(database, read_only=True) as uow:
        missing = []
        for table in sorted(_SIGN_IN_TABLES):
            columns = {
                str(row["name"])
                for row in uow.query_all(f"PRAGMA table_info({table})")
            }
            required = _SIGN_IN_SCHEMA.get(table, set())
            if not columns or not required.issubset(columns):
                missing.append(table)
    return tuple(missing)


def create_blueprint(*, context=None, application=None, permission=None) -> Blueprint:
    blueprint = Blueprint("activity", __name__)
    resolver = permission or (lambda _required: True)

    @blueprint.get("/api/v1/activity")
    @guard("admin", resolver)
    def activity_status():
        logger = getattr(context, "database", None)
        if logger is None:
            return api_success({"items": [], "source": "compatibility"})
        from ....infrastructure.observability import AuditLogger
        return api_success({"items": AuditLogger(logger.path("game_db")).list(), "source": "web_audit"})

    @blueprint.get("/api/v1/activity/state")
    @guard("user", resolver)
    def activity_state():
        """Read-only activity state; claiming keeps its own write route."""
        if application is None:
            return api_success({"items": [], "source": "compatibility"})
        # The panel fetches this endpoint without parameters, so an absent
        # user_id renders the activity card without a personal progress line.
        user_id = str(request.args.get("user_id") or "").strip()
        if user_id and not resolver("admin"):
            return api_error(
                "forbidden", "查看个人活动进度需要管理权限", status=403
            )
        view = str(request.args.get("view") or "center").strip() or "center"
        read_model = application.read_model
        views = {
            "center": read_model.activity_center_text,
            "detail": read_model.activity_info_text,
            "stage": read_model.activity_stage_text,
            "rank": read_model.rank_overview_text,
            "boss": read_model.boss_status_text,
            "shop": read_model.point_shop_guidance_text,
            "bag": read_model.collect_bag_guidance_text,
            "reward": read_model.reward_catalog_text,
            "gameplay": read_model.gameplay_personal_text,
        }
        render = views.get(view)
        if render is None:
            return api_error("validation_error", f"unknown view: {view}", status=400)
        return api_success({"view": view, "user_id": user_id, "text": render(user_id)})

    @blueprint.post("/api/v1/activity/sign-in")
    @guard("admin", resolver, write=True)
    def activity_sign_in():
        if application is None:
            return api_error("unavailable", "活動簽到服務不可用", status=503)
        payload = request.get_json(silent=True)
        if not isinstance(payload, dict):
            return api_error("validation_error", "请求内容必须是 JSON 对象", status=400)
        user_id = str(payload.get("user_id") or "").strip()
        operation_id = str(
            request.headers.get("Idempotency-Key") or ""
        ).strip()
        if not user_id or len(user_id) > 128:
            return api_error("validation_error", "玩家 ID 必须为 1 至 128 个字符", status=400)
        if not operation_id or len(operation_id) > 128:
            return api_error("validation_error", "Idempotency-Key 必须为 1 至 128 个字符", status=400)

        database = str(getattr(application, "database", "") or "").strip()
        database_path = Path(database) if database else None
        if database_path is None or not database_path.is_file():
            return api_error(
                "migrations_required", "活动签到数据库尚未就绪，请先完成启动迁移", status=503
            )
        try:
            missing = _missing_sign_schema(database_path)
        except sqlite3.Error:
            return api_error("storage_unavailable", "活动签到数据库暂时不可用", status=503)
        if missing:
            return api_error(
                "migrations_required",
                "活动签到数据结构尚未就绪，请先完成启动迁移",
                details={"missing": list(missing)},
                status=503,
            )

        try:
            outcome = application.execute(
                operation_id=operation_id,
                user_id=user_id,
                payload={"action": "claim_sign"},
            )
        except sqlite3.OperationalError as exc:
            message = str(exc).casefold()
            if "no such table" in message or "no such column" in message:
                return api_error(
                    "migrations_required",
                    "活动签到数据结构尚未就绪，请先完成启动迁移",
                    status=503,
                )
            raise
        if not outcome.ok:
            return api_error(
                outcome.code or "rejected",
                outcome.message or "活动签到未完成",
                details=outcome.to_dict(),
                status=409,
            )
        return api_success(outcome.to_dict())

    return blueprint


__all__ = ["create_blueprint"]
