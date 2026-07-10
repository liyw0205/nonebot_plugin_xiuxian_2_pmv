from __future__ import annotations

import asyncio
import time
from dataclasses import dataclass
from typing import Any

from nonebot.log import logger


@dataclass(frozen=True)
class RegistrationRequest:
    user_id: str
    root: str
    root_type: str
    power: int
    create_time: str
    user_name: str


class RegistrationBatcher:
    def __init__(self, manager, *, max_batch_size: int = 200, flush_delay: float = 0.01) -> None:
        self._manager = manager
        self._max_batch_size = max(1, int(max_batch_size))
        self._flush_delay = max(0.0, float(flush_delay))
        self._queue: asyncio.Queue[tuple[str, RegistrationRequest, asyncio.Future]] = asyncio.Queue()
        self._worker: asyncio.Task | None = None
        self._lock = asyncio.Lock()

    async def submit(self, request: RegistrationRequest) -> tuple[Any, str]:
        loop = asyncio.get_running_loop()
        future = loop.create_future()
        request_id = f"{id(future)}:{time.time_ns()}"
        await self._ensure_worker()
        await self._queue.put((request_id, request, future))
        return await future

    async def _ensure_worker(self) -> None:
        if self._worker and not self._worker.done():
            return
        async with self._lock:
            if not self._worker or self._worker.done():
                self._worker = asyncio.create_task(self._run(), name="xiuxian-registration-batcher")

    async def _run(self) -> None:
        while True:
            first = await self._queue.get()
            batch = [first]
            deadline = time.monotonic() + self._flush_delay
            while len(batch) < self._max_batch_size:
                timeout = deadline - time.monotonic()
                if timeout <= 0:
                    break
                try:
                    batch.append(await asyncio.wait_for(self._queue.get(), timeout=timeout))
                except asyncio.TimeoutError:
                    break
            await self._flush(batch)

    async def _flush(self, batch) -> None:
        rows = []
        for request_id, request, _future in batch:
            rows.append({"request_id": request_id, **request.__dict__})
        try:
            results = await asyncio.to_thread(self._manager.create_users_batch_fast, rows)
        except Exception as exc:
            logger.exception(f"批量注册任务失败: {exc}")
            results = {}
        for request_id, _request, future in batch:
            if not future.done():
                future.set_result(results.get(request_id, (False, "注册失败，请稍后重试。")))
            self._queue.task_done()


__all__ = ["RegistrationBatcher", "RegistrationRequest"]
