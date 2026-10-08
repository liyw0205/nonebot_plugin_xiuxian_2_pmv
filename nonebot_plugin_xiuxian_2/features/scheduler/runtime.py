from __future__ import annotations

from .application import SchedulerAdminApplication, SchedulerAdminManager


def build_scheduler_admin_application(
    manager: SchedulerAdminManager | None = None,
) -> SchedulerAdminApplication:
    if manager is None:
        from ...xiuxian.xiuxian_scheduler import job_manager

        manager = job_manager
    return SchedulerAdminApplication(manager)


scheduler_admin_application = build_scheduler_admin_application()


__all__ = ["build_scheduler_admin_application", "scheduler_admin_application"]
