from flask import Blueprint, request
from ..api import api_success
from ._common import guard


def create_blueprint(*, context=None, permission=None) -> Blueprint:
    blueprint = Blueprint("config", __name__)
    resolver = permission or (lambda _required: True)

    @blueprint.get("/api/v1/config")
    @guard("admin", resolver)
    def get_config():
        settings = getattr(context, "settings", None)
        return api_success(settings.summary() if settings is not None else {})

    @blueprint.post("/api/v1/config")
    @guard("admin", resolver, write=True)
    def update_config():
        settings = getattr(context, "settings", None)
        service = getattr(context, "config_service", None)
        if service is None:
            return api_success({"updated": False, "reason": "config_service_unavailable"})
        payload = request.get_json(silent=True)
        if not isinstance(payload, dict):
            from ..api import api_error
            return api_error("validation_error", "请求体必须是 JSON 对象", status=400)
        try:
            context.settings = service.update(payload)
        except (KeyError, ValueError) as exc:
            from ..api import api_error
            return api_error("validation_error", str(exc), status=400)
        return api_success(context.settings.summary())

    return blueprint


__all__ = ["create_blueprint"]
