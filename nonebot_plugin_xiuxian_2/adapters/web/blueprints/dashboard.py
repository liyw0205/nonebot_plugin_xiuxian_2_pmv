from flask import Blueprint
from ..api import api_success


def create_blueprint(*, context=None, registry=None, readiness=None) -> Blueprint:
    blueprint = Blueprint("dashboard", __name__)

    @blueprint.get("/api/v1/dashboard")
    def dashboard_api():
        report = readiness.report().to_dict() if readiness is not None else {"ready": False, "checks": {}}
        tracker = getattr(context, "compatibility", None)
        hits = tracker() if callable(tracker) else {}
        return api_success({"status": "ready" if report["ready"] else "starting", "readiness": report, "registry": registry.export() if registry is not None else {}, "compatibility_hits": hits})

    return blueprint


__all__ = ["create_blueprint"]
