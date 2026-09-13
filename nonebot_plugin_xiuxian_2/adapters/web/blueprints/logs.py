from flask import Blueprint

from ..api import api_error, api_success
from ._common import guard


def create_blueprint(*, context=None, permission=None) -> Blueprint:
    blueprint = Blueprint("logs", __name__)
    resolver = permission or (lambda _required: True)

    @blueprint.get("/api/v1/logs")
    @guard("admin", resolver)
    def logs():
        catalog = getattr(context, "database", None)
        if catalog is None:
            return api_error("unavailable", "日志存储不可用", status=503)
        from ....infrastructure.observability import AuditLogger

        return api_success(AuditLogger(catalog.path("game_db")).list())

    return blueprint


__all__ = ["create_blueprint"]
