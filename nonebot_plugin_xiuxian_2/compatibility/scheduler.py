"""Deferred bridge for legacy APScheduler declarations.

Legacy modules still use ``scheduled_job`` decorators, but decorators must not
mutate the live scheduler while modules are being imported.  The bridge keeps
the old syntax working, records declarations, and activates them from the
composition root after the manifest and runtime are ready.
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Callable


@dataclass(frozen=True)
class _PendingJob:
    function: Callable[..., Any]
    args: tuple[Any, ...]
    kwargs: dict[str, Any]

    @property
    def job_id(self) -> str | None:
        value = self.kwargs.get("id")
        return str(value) if value is not None else None


class DeferredScheduler:
    """Proxy that defers registration while legacy modules are imported."""

    def __init__(self, scheduler: Any) -> None:
        self._scheduler = scheduler
        self._pending: list[_PendingJob] = []
        self._registered: set[str] = set()
        self._active = False

    @property
    def active(self) -> bool:
        return self._active

    @property
    def pending_ids(self) -> tuple[str, ...]:
        return tuple(item.job_id for item in self._pending if item.job_id)

    @property
    def registered_ids(self) -> tuple[str, ...]:
        return tuple(sorted(self._registered))

    def scheduled_job(self, *args: Any, **kwargs: Any):
        def decorator(function: Callable[..., Any]) -> Callable[..., Any]:
            pending = _PendingJob(function, tuple(args), dict(kwargs))
            if self._active:
                self._register(pending)
            else:
                self._pending.append(pending)
            return function

        return decorator

    def add_job(self, function: Callable[..., Any] | None = None, *args: Any, **kwargs: Any):
        # APScheduler also exposes ``func=`` as a keyword; legacy startup
        # callbacks use that form for dynamically-created jobs.
        if function is None:
            function = kwargs.pop("func", None)
        if not callable(function):
            raise TypeError("add_job() requires a callable function")
        pending = _PendingJob(function, tuple(args), dict(kwargs))
        if not self._active:
            self._pending.append(pending)
            return None
        return self._register(pending)

    def activate(self) -> None:
        if self._active:
            return
        self._active = True
        pending = tuple(self._pending)
        self._pending.clear()
        for item in pending:
            self._register(item)

    def _register(self, pending: _PendingJob):
        job_id = pending.job_id
        if job_id and job_id in self._registered:
            return self._scheduler.get_job(job_id)
        if job_id:
            existing = self._scheduler.get_job(job_id)
            if existing is not None:
                raise RuntimeError(f"duplicate legacy scheduler job id: {job_id}")
        job = self._scheduler.add_job(pending.function, *pending.args, **pending.kwargs)
        if job_id:
            self._registered.add(job_id)
        return job

    def __getattr__(self, name: str) -> Any:
        return getattr(self._scheduler, name)


_bridge: DeferredScheduler | None = None


def install_scheduler_bridge(module: Any) -> DeferredScheduler:
    """Replace a loaded APScheduler module's scheduler with a deferred proxy."""
    global _bridge
    existing = getattr(module, "_xiuxian_scheduler_bridge", None)
    if isinstance(existing, DeferredScheduler):
        _bridge = existing
        return existing
    scheduler = getattr(module, "scheduler", None)
    if isinstance(scheduler, DeferredScheduler):
        _bridge = scheduler
        return scheduler
    if scheduler is None:
        raise RuntimeError("nonebot_plugin_apscheduler has no scheduler")
    bridge = DeferredScheduler(scheduler)
    module._xiuxian_real_scheduler = scheduler
    module._xiuxian_scheduler_bridge = bridge
    module.scheduler = bridge
    _bridge = bridge
    return bridge


def activate_scheduler_bridge() -> DeferredScheduler | None:
    if _bridge is None:
        return None
    _bridge.activate()
    return _bridge


def scheduler_bridge() -> DeferredScheduler | None:
    return _bridge


__all__ = [
    "DeferredScheduler",
    "activate_scheduler_bridge",
    "install_scheduler_bridge",
    "scheduler_bridge",
]
