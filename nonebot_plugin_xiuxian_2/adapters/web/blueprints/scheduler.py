from flask import Blueprint, request
from ..api import api_success
from ._common import guard


def create_blueprint(*, context=None, permission=None) -> Blueprint:
    blueprint = Blueprint("scheduler", __name__)
    resolver = permission or (lambda _required: True)

    @blueprint.get("/api/v1/scheduler")
    @guard("admin", resolver)
    def scheduler_status():
        registry = getattr(context, "jobs", None)
        return api_success({"jobs": registry.export() if registry is not None else [], "count": len(registry.list()) if registry is not None else 0})

    @blueprint.post("/api/v1/scheduler/<job_id>/run")
    @guard("admin", resolver, write=True)
    def run_job(job_id: str):
        registry = getattr(context, "jobs", None)
        executor = getattr(context, "job_executor", None)
        if registry is None or executor is None:
            from ..api import api_error
            return api_error("unavailable", "调度器不可用", status=503)
        try:
            registry.get(job_id)
        except KeyError:
            from ..api import api_error
            return api_error("not_found", "任务不存在", status=404)
        payload = request.get_json(silent=True) or {}
        scheduled_at = str(payload.get("scheduled_at") or "manual")
        result = executor.run_sync(job_id, scheduled_at)
        return api_success(result.__dict__, status=200 if result.status not in {"failed"} else 500)

    return blueprint


__all__ = ["create_blueprint"]
