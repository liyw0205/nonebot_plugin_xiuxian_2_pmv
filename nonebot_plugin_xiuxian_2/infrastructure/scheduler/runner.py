from __future__ import annotations

import asyncio
import inspect
import threading
from time import monotonic
from dataclasses import dataclass
from typing import Any

from ..observability import emit, trace_context
from .registry import JobDefinition, JobRegistry


@dataclass(frozen=True)
class JobRunResult:
    job_id: str
    status: str
    attempts: int = 0
    error: str | None = None


class JobExecutor:
    """Adapter-independent executor enforcing stable ID and retry policy."""

    def __init__(self, registry: JobRegistry) -> None:
        self.registry = registry
        self._running: set[str] = set()
        self._completed: set[str] = set()
        # ``asyncio.Lock`` is tied to an event loop after contention.  A
        # scheduler is also invoked by CLI/tests via multiple ``asyncio.run``
        # calls, so state coordination must not be loop-owned.
        self._lock = threading.RLock()

    async def run(self, job_id: str, scheduled_at: str, *args: Any, **kwargs: Any) -> JobRunResult:
        job = self.registry.get(job_id)
        key = job.idempotency_key.format(job_id=job.id, scheduled_at=scheduled_at)
        started = monotonic()

        def report(result: JobRunResult) -> JobRunResult:
            emit(
                "info" if result.status in {"applied", "replayed", "skipped"} else "warning",
                "job execution",
                operation_id=key,
                job_id=job.id,
                duration_ms=int((monotonic() - started) * 1000),
                job_status=result.status,
                attempts=result.attempts,
                error=result.error or "",
            )
            return result

        with self._lock:
            if key in self._completed:
                return report(JobRunResult(job.id, "replayed"))
            if job.id in self._running and job.concurrency_policy == "skip":
                return report(JobRunResult(job.id, "skipped"))
            self._running.add(job.id)
        attempts = 0
        retries = _retry_count(job.retry_policy)
        try:
            with trace_context(operation_id=key, job_id=job.id):
                while True:
                    attempts += 1
                    try:
                        handler_kwargs = dict(kwargs)
                        # New handlers may opt into the scheduler's stable
                        # timestamp without breaking legacy callables that do
                        # not accept keyword arguments.
                        try:
                            parameters = inspect.signature(job.handler).parameters
                        except (TypeError, ValueError):
                            parameters = {}
                        if "scheduled_at" in parameters and "scheduled_at" not in handler_kwargs:
                            handler_kwargs["scheduled_at"] = scheduled_at
                        if inspect.iscoroutinefunction(job.handler):
                            await asyncio.wait_for(job.handler(*args, **handler_kwargs), timeout=job.timeout)
                        else:
                            result = await asyncio.wait_for(asyncio.to_thread(job.handler, *args, **handler_kwargs), timeout=job.timeout)
                            if inspect.isawaitable(result):
                                await asyncio.wait_for(result, timeout=job.timeout)
                        with self._lock:
                            self._completed.add(key)
                        return report(JobRunResult(job.id, "applied", attempts))
                    except Exception as exc:
                        if attempts > retries:
                            return report(JobRunResult(job.id, "failed", attempts, str(exc)))
        finally:
            with self._lock:
                self._running.discard(job.id)

    def run_sync(self, job_id: str, scheduled_at: str, *args: Any, **kwargs: Any) -> JobRunResult:
        """Run from Flask/CLI code regardless of the caller's event loop.

        Flask routes are synchronous, but an embedding test server or an
        async host may already have a running loop in the same thread.  A
        short-lived worker thread gives ``asyncio.run`` its own loop without
        nesting or reusing loop-bound state.
        """
        try:
            asyncio.get_running_loop()
        except RuntimeError:
            return asyncio.run(self.run(job_id, scheduled_at, *args, **kwargs))

        result: list[JobRunResult] = []
        error: list[BaseException] = []

        def worker() -> None:
            try:
                result.append(asyncio.run(self.run(job_id, scheduled_at, *args, **kwargs)))
            except BaseException as exc:  # propagate the original failure
                error.append(exc)

        thread = threading.Thread(target=worker, name=f"job-{job_id}", daemon=True)
        thread.start()
        thread.join()
        if error:
            raise error[0]
        return result[0]


def _retry_count(policy: str) -> int:
    text = str(policy).casefold()
    if text.isdigit():
        return int(text)
    if text.startswith("retry:") and text[6:].isdigit():
        return int(text[6:])
    return 0


__all__ = ["JobExecutor", "JobRunResult"]
