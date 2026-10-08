from __future__ import annotations

from typing import Any, Protocol


class SchedulerAdminManager(Protocol):
    def list_jobs(self) -> list[dict[str, Any]]: ...

    def set_enabled(self, job_id: str, enabled: bool) -> dict[str, Any]: ...

    def reschedule(self, job_id: str, trigger_spec: object) -> dict[str, Any]: ...

    def queue_manual_run(self, job_id: str) -> dict[str, Any]: ...

    def get_run(self, run_id: str) -> dict[str, Any]: ...


class SchedulerAdminApplication:
    """Use-case boundary for the legacy APScheduler admin controls."""

    def __init__(self, manager: SchedulerAdminManager) -> None:
        self._manager = manager

    def list_jobs(self) -> list[dict[str, Any]]:
        return self._manager.list_jobs()

    def set_enabled(self, job_id: str, enabled: bool) -> dict[str, Any]:
        return self._manager.set_enabled(job_id, enabled)

    def reschedule(self, job_id: str, trigger_spec: object) -> dict[str, Any]:
        return self._manager.reschedule(job_id, trigger_spec)

    def queue_manual_run(self, job_id: str) -> dict[str, Any]:
        return self._manager.queue_manual_run(job_id)

    def get_run(self, run_id: str) -> dict[str, Any]:
        return self._manager.get_run(run_id)


__all__ = ["SchedulerAdminApplication", "SchedulerAdminManager"]
