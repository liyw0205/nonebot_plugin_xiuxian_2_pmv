"""Bound live rank scans without blocking the application's event loop."""
from __future__ import annotations

import asyncio
from concurrent.futures import ThreadPoolExecutor, wait
from contextvars import copy_context
import threading


class ExactRankReader:
    def __init__(self):
        self._executor = ThreadPoolExecutor(max_workers=2, thread_name_prefix="xiuxian-exact-rank")
        self._loop = None
        self._slots = None
        self._requests = set()
        self._workers = set()
        self._worker_lock = threading.Lock()

    def _worker_done(self, future):
        with self._worker_lock:
            self._workers.discard(future)

    def wait(self):
        # Concurrent futures complete in the worker, independently of the event loop.
        with self._worker_lock:
            workers = tuple(self._workers)
        if workers:
            wait(workers)

    def _bind(self):
        loop = asyncio.get_running_loop()
        if loop is not self._loop:
            if self._requests:
                raise RuntimeError("exact rank reads must drain before changing event loops")
            self._loop, self._slots = loop, asyncio.Semaphore(2)
        return loop

    async def read(self, manager, user_id):
        loop = self._bind()
        finished = loop.create_future()
        self._requests.add(finished)
        try:
            async with self._slots:
                context = copy_context()
                worker = self._executor.submit(context.run, manager.get_exp_and_stone_ranks, user_id)
                with self._worker_lock:
                    self._workers.add(worker)
                worker.add_done_callback(self._worker_done)
                future = asyncio.wrap_future(worker, loop=loop)
                cancelled = False
                # Cancellation cannot release a slot while SQLite is still reading.
                while not future.done():
                    try:
                        await asyncio.shield(future)
                    except asyncio.CancelledError:
                        cancelled = True
                    except Exception:
                        break
                if cancelled:
                    if not future.cancelled():
                        future.exception()
                    raise asyncio.CancelledError
                return future.result()
        finally:
            self._requests.discard(finished)
            finished.set_result(None)

    async def drain(self):
        self._bind()
        cancelled = False
        while self._requests:
            future = next(iter(self._requests))
            try:
                await asyncio.shield(future)
            except asyncio.CancelledError:
                cancelled = True
        if cancelled:
            raise asyncio.CancelledError

    def close(self):
        self._executor.shutdown(wait=True)


_reader = ExactRankReader()


async def run_exact_rank_read(manager, user_id):
    return await _reader.read(manager, user_id)


async def drain_exact_rank_reads():
    await _reader.drain()


def wait_for_exact_rank_reads():
    _reader.wait()
