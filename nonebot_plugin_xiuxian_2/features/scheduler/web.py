"""Web surface of the scheduler administration owner.

``ROUTES`` stays empty on purpose.  The five JSON paths below are still
registered by the legacy Flask module ``xiuxian/xiuxian_web/scheduler.py`` behind
the admin session and ``WebPermission.SCHEDULER``; declaring them here too would
be a second owner for one path.  ``GET /scheduler`` renders ``scheduler.html``
without reaching any use case, so it is not a delegation and is not listed.
"""

ROUTES = ()
LEGACY_ROUTES = (
    ("GET", "/api/scheduler/jobs", "SchedulerAdminApplication.list_jobs"),
    ("POST", "/api/scheduler/jobs/<job_id>/enabled", "SchedulerAdminApplication.set_enabled"),
    ("POST", "/api/scheduler/jobs/<job_id>/schedule", "SchedulerAdminApplication.reschedule"),
    ("POST", "/api/scheduler/jobs/<job_id>/run", "SchedulerAdminApplication.queue_manual_run"),
    ("GET", "/api/scheduler/runs/<run_id>", "SchedulerAdminApplication.get_run"),
)

__all__ = ["LEGACY_ROUTES", "ROUTES"]
