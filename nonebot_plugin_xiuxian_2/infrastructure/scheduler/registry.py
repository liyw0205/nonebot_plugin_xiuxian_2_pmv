from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Callable, Iterable

from ...bootstrap.registry import JobSpec


@dataclass(frozen=True)
class JobDefinition:
    id: str
    title: str
    owner: str
    schedule: str
    handler: Callable[..., Any]
    misfire_policy: str = "coalesce"
    concurrency_policy: str = "skip"
    timeout: float = 60
    retry_policy: str = "none"
    idempotency_key: str = "{job_id}:{scheduled_at}"

    def __post_init__(self) -> None:
        if not all(str(getattr(self, name)).strip() for name in ("id", "title", "owner", "schedule")):
            raise ValueError("job id, title, owner and schedule are required")
        if self.timeout <= 0:
            raise ValueError("job timeout must be positive")


class JobRegistry:
    def __init__(self) -> None:
        self._jobs: dict[str, JobDefinition] = {}

    def register(self, job: JobDefinition) -> JobDefinition:
        previous = self._jobs.get(job.id)
        if previous is not None:
            if _job_metadata(previous) != _job_metadata(job):
                raise ValueError(f"conflicting job id: {job.id}")
            return previous
        self._jobs[job.id] = job
        return job

    def register_many(self, jobs: Iterable[JobDefinition]) -> None:
        for job in jobs:
            self.register(job)

    def register_manifest(self, spec: JobSpec, handler: Callable[..., Any] | None = None) -> JobDefinition:
        return self.register(
            JobDefinition(
                id=spec.id,
                title=spec.title,
                owner=spec.owner,
                schedule=spec.schedule,
                handler=handler or _unwired_handler(spec.id),
                misfire_policy=spec.misfire_policy,
                concurrency_policy=spec.concurrency_policy,
                timeout=spec.timeout,
                retry_policy=spec.retry_policy,
                idempotency_key=spec.idempotency_key,
            )
        )

    def get(self, job_id: str) -> JobDefinition:
        return self._jobs[job_id]

    def list(self) -> tuple[JobDefinition, ...]:
        return tuple(self._jobs.values())

    def export(self) -> list[dict[str, Any]]:
        return [
            {
                "id": job.id,
                "title": job.title,
                "owner": job.owner,
                "schedule": job.schedule,
                "misfire_policy": job.misfire_policy,
                "concurrency_policy": job.concurrency_policy,
                "timeout": job.timeout,
                "retry_policy": job.retry_policy,
                "idempotency_key": job.idempotency_key,
            }
            for job in self.list()
        ]


def _unwired_handler(job_id: str) -> Callable[..., Any]:
    def handler(*args: Any, **kwargs: Any) -> None:
        raise RuntimeError(f"job is declared but not wired: {job_id}")

    return handler


def _job_metadata(job: JobDefinition) -> tuple[Any, ...]:
    return (
        job.id,
        job.title,
        job.owner,
        job.schedule,
        job.misfire_policy,
        job.concurrency_policy,
        job.timeout,
        job.retry_policy,
        job.idempotency_key,
    )


__all__ = ["JobDefinition", "JobRegistry"]
