from __future__ import annotations

import asyncio
import inspect
from dataclasses import dataclass
from collections.abc import Awaitable, Callable
from typing import Any, Literal

from nonebot.log import logger

from .metrics import RuntimeMetrics, runtime_metrics


OverflowPolicy = Literal["drop", "wait", "raise"]
Job = Callable[[], Awaitable[Any] | Any]


@dataclass(frozen=True)
class _QueuedJob:
    operation: Job
    max_retries: int = 0


class BackgroundJobQueue:
    def __init__(
        self,
        name: str,
        *,
        max_size: int = 5000,
        workers: int = 4,
        overflow_policy: OverflowPolicy = "drop",
        metrics: RuntimeMetrics | None = None,
    ) -> None:
        if max_size <= 0 or workers <= 0:
            raise ValueError("max_size 和 workers 必须大于 0")
        if overflow_policy not in {"drop", "wait", "raise"}:
            raise ValueError(f"未知溢出策略: {overflow_policy}")
        self.name = name
        self.worker_count = workers
        self.overflow_policy = overflow_policy
        self._queue: asyncio.Queue[_QueuedJob | None] = asyncio.Queue(maxsize=max_size)
        self._workers: list[asyncio.Task[None]] = []
        self._metrics = metrics or runtime_metrics

    def _metric(self, suffix: str) -> str:
        return f"queue.{self.name}.{suffix}"

    @property
    def dropped(self) -> int:
        return self._metrics.get(self._metric("dropped"))

    @property
    def completed(self) -> int:
        return self._metrics.get(self._metric("completed"))

    @property
    def failed(self) -> int:
        return self._metrics.get(self._metric("failed"))

    @property
    def retried(self) -> int:
        return self._metrics.get(self._metric("retried"))

    @property
    def size(self) -> int:
        return self._queue.qsize()

    def metrics_snapshot(self) -> dict[str, int]:
        self._metrics.set(self._metric("size"), self.size)
        return self._metrics.snapshot(prefix=f"queue.{self.name}.")

    @property
    def running(self) -> bool:
        return bool(self._workers)

    async def start(self) -> None:
        if self._workers:
            return
        self._workers = [
            asyncio.create_task(self._worker(), name=f"{self.name}-{index}")
            for index in range(self.worker_count)
        ]

    async def submit(
        self,
        job: Job,
        *,
        critical: bool = False,
        max_retries: int = 0,
    ) -> bool:
        if not callable(job):
            raise TypeError("job 必须是无参数可调用对象")
        if max_retries < 0:
            raise ValueError("max_retries 不能小于 0")
        queued = _QueuedJob(job, int(max_retries))
        if critical or self.overflow_policy == "wait":
            await self._queue.put(queued)
            self._metrics.increment(self._metric("submitted"))
            return True
        try:
            self._queue.put_nowait(queued)
            self._metrics.increment(self._metric("submitted"))
            return True
        except asyncio.QueueFull:
            if self.overflow_policy == "raise":
                raise
            dropped = self._metrics.increment(self._metric("dropped"))
            if dropped == 1 or dropped % 100 == 0:
                logger.warning(f"[后台队列:{self.name}] 队列已满，累计丢弃 {dropped} 个非关键任务")
            return False

    async def join(self) -> None:
        await self._queue.join()

    async def stop(self, *, drain: bool = True) -> None:
        if not self._workers:
            return
        if drain:
            await self.join()
        for _ in self._workers:
            await self._queue.put(None)
        workers, self._workers = self._workers, []
        await asyncio.gather(*workers, return_exceptions=True)

    async def _worker(self) -> None:
        while True:
            queued = await self._queue.get()
            try:
                if queued is None:
                    return
                for attempt in range(queued.max_retries + 1):
                    try:
                        result = queued.operation()
                        if inspect.isawaitable(result):
                            await result
                        self._metrics.increment(self._metric("completed"))
                        break
                    except Exception:
                        if attempt >= queued.max_retries:
                            raise
                        self._metrics.increment(self._metric("retried"))
                        await asyncio.sleep(0)
            except Exception as exc:
                self._metrics.increment(self._metric("failed"))
                logger.exception(f"[后台队列:{self.name}] 任务执行失败: {exc}")
            finally:
                self._queue.task_done()


__all__ = ["BackgroundJobQueue", "Job", "OverflowPolicy"]
