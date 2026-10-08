from __future__ import annotations

from ...features.scheduler.runtime import scheduler_admin_application
from .core import api_error, api_success, app, render_template, request


@app.route("/scheduler")
def scheduler_management():
    return render_template("scheduler.html")


@app.route("/api/scheduler/jobs")
def api_scheduler_jobs():
    return api_success(jobs=scheduler_admin_application.list_jobs())


@app.route("/api/scheduler/jobs/<job_id>/enabled", methods=["POST"])
def api_scheduler_job_enabled(job_id):
    data = request.get_json(silent=True) or {}
    enabled = data.get("enabled")
    if not isinstance(enabled, bool):
        return api_error("enabled 必须是布尔值", status=400)
    try:
        return api_success(job=scheduler_admin_application.set_enabled(job_id, enabled))
    except ValueError as exc:
        return api_error(exc, status=400)


@app.route("/api/scheduler/jobs/<job_id>/schedule", methods=["POST"])
def api_scheduler_job_schedule(job_id):
    data = request.get_json(silent=True) or {}
    try:
        return api_success(job=scheduler_admin_application.reschedule(job_id, data.get("trigger")))
    except ValueError as exc:
        return api_error(exc, status=400)


@app.route("/api/scheduler/jobs/<job_id>/run", methods=["POST"])
def api_scheduler_job_run(job_id):
    try:
        return api_success(**scheduler_admin_application.queue_manual_run(job_id))
    except ValueError as exc:
        return api_error(exc, status=400)


@app.route("/api/scheduler/runs/<run_id>")
def api_scheduler_run(run_id):
    try:
        return api_success(run=scheduler_admin_application.get_run(run_id))
    except ValueError as exc:
        return api_error(exc, status=404)
