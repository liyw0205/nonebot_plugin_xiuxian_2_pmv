from flask import Blueprint
from ..api import api_success
from ._common import guard


def create_blueprint(*, context=None, permission=None) -> Blueprint:
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

    return blueprint


__all__ = ["create_blueprint"]
