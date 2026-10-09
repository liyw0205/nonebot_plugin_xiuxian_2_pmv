"""Ports the scheduler administration owner needs.

``SchedulerAdminManager`` is the whole dependency of
``SchedulerAdminApplication``.  In production the implementation is the legacy
``SchedulerJobManager`` (``features/scheduler/apscheduler_manager.py``), reached
through the re-export in ``xiuxian/xiuxian_scheduler/job_manager.py``; the Web
layer injects it via ``runtime.build_scheduler_admin_application``.  Keeping the
Protocol here states that the use case depends on a manager port, not on
APScheduler.
"""

from __future__ import annotations

from typing import Any, Protocol


class SchedulerAdminManager(Protocol):
    def list_jobs(self) -> list[dict[str, Any]]: ...

    def set_enabled(self, job_id: str, enabled: bool) -> dict[str, Any]: ...

    def reschedule(self, job_id: str, trigger_spec: object) -> dict[str, Any]: ...

    def queue_manual_run(self, job_id: str) -> dict[str, Any]: ...

    def get_run(self, run_id: str) -> dict[str, Any]: ...


__all__ = ["SchedulerAdminManager"]
