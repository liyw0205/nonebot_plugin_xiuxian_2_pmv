from __future__ import annotations

import asyncio
import inspect
from collections.abc import Awaitable, Callable
from typing import Any, Literal

from nonebot.log import logger


OverflowPolicy = Literal["drop", "wait", "raise"]
Job = Callable[[], Awaitable[Any] | Any]


class BackgroundJobQueue:
    def __init__(
        self,
        name: str,
        *,
        max_size: int = 5000,
        workers: int = 4,
        overflow_policy: OverflowPolicy = "drop",
    ) -> None:
        if max_size <= 0 or workers <= 0:
            raise ValueError("max_size 和 workers 必须大于 0")
        if overflow_policy not in {"drop", "wait", "raise"}:
            raise ValueError(f"未知溢出策略: {overflow_policy}")
        self.name = name
        self.worker_count = workers
        self.overflow_policy = overflow_policy
        self._queue: asyncio.Queue[Job | None] = asyncio.Queue(maxsize=max_size)
        self._workers: list[asyncio.Task[None]] = []
        self.dropped = 0

    @property
    def size(self) -> int:
        return self._queue.qsize()

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

    async def submit(self, job: Job) -> bool:
        if not callable(job):
            raise TypeError("job 必须是无参数可调用对象")
        if self.overflow_policy == "wait":
            await self._queue.put(job)
            return True
        try:
            self._queue.put_nowait(job)
            return True
        except asyncio.QueueFull:
            if self.overflow_policy == "raise":
                raise
            self.dropped += 1
            if self.dropped == 1 or self.dropped % 100 == 0:
                logger.warning(f"[后台队列:{self.name}] 队列已满，累计丢弃 {self.dropped} 个非关键任务")
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
            job = await self._queue.get()
            try:
                if job is None:
                    return
                result = job()
                if inspect.isawaitable(result):
                    await result
            except Exception as exc:
                logger.exception(f"[后台队列:{self.name}] 任务执行失败: {exc}")
            finally:
                self._queue.task_done()


__all__ = ["BackgroundJobQueue", "Job", "OverflowPolicy"]
