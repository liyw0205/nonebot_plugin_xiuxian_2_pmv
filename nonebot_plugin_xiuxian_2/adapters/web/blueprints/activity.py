from flask import Blueprint, request

from ..api import api_error, api_success
from ._common import guard


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
        view = str(request.args.get("view") or "center").strip() or "center"
        read_model = application.read_model
        views = {
            "center": read_model.activity_center_text,
            "stage": read_model.activity_stage_text,
            "rank": read_model.rank_overview_text,
            "boss": read_model.boss_status_text,
        }
        render = views.get(view)
        if render is None:
            return api_error("validation_error", f"unknown view: {view}", status=400)
        return api_success({"view": view, "user_id": user_id, "text": render(user_id)})

    return blueprint


__all__ = ["create_blueprint"]
