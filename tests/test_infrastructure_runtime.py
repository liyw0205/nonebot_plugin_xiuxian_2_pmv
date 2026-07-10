from __future__ import annotations

import asyncio
import unittest
from types import SimpleNamespace

import nonebot

nonebot.init()

from nonebot_plugin_xiuxian_2.xiuxian.infrastructure import (
    BackgroundJobQueue,
    QQEventDeduplicator,
    RuntimeMetrics,
    TTLStore,
)


class InfrastructureRuntimeTests(unittest.IsolatedAsyncioTestCase):
    async def test_ttl_store_expires_and_evicts(self) -> None:
        now = [0.0]
        store = TTLStore[str, int](ttl=10, max_size=2, clock=lambda: now[0])
        await store.set("a", 1)
        await store.set("b", 2)
        await store.set("c", 3)
        self.assertIsNone(await store.get("a"))
        self.assertEqual(await store.get("b"), 2)
        now[0] = 11
        self.assertIsNone(await store.get("b"))

    async def test_event_dedup_uses_bot_and_event_type(self) -> None:
        class Event:
            __module__ = "nonebot.adapters.qq.event"
            event_type = "GROUP_MESSAGE_CREATE"
            id = "message-1"
            event_id = None
            content = "hello"
            group_openid = "group-1"
            author = SimpleNamespace(id="user-1", member_openid="member-1")

            def get_event_name(self):
                return self.event_type

            def get_message(self):
                return self.content

        metrics = RuntimeMetrics()
        dedup = QQEventDeduplicator(metrics=metrics)
        bot = SimpleNamespace(self_id="bot-1")
        self.assertFalse(await dedup.is_duplicate(bot, Event()))
        self.assertTrue(await dedup.is_duplicate(bot, Event()))
        self.assertFalse(await dedup.is_duplicate(SimpleNamespace(self_id="bot-2"), Event()))

        event_without_id = Event()
        event_without_id.id = ""
        self.assertFalse(await dedup.is_duplicate(bot, event_without_id))
        self.assertEqual(metrics.get("dedup.checked"), 3)
        self.assertEqual(metrics.get("dedup.hit"), 1)
        self.assertFalse(await dedup.is_duplicate(bot, event_without_id))
        self.assertEqual(metrics.get("dedup.skipped_no_stable_id"), 2)

    async def test_background_queue_runs_jobs_and_tracks_drops(self) -> None:
        gate = asyncio.Event()
        executed: list[int] = []
        queue = BackgroundJobQueue("test", max_size=1, workers=1, overflow_policy="drop")

        async def blocked_job():
            await gate.wait()
            executed.append(1)

        self.assertTrue(await queue.submit(blocked_job))
        self.assertFalse(await queue.submit(blocked_job))
        self.assertEqual(queue.dropped, 1)
        await queue.start()
        gate.set()
        await queue.stop(drain=True)
        self.assertEqual(executed, [1])
        self.assertEqual(queue.completed, 1)

    async def test_critical_job_waits_for_capacity_instead_of_dropping(self) -> None:
        queue = BackgroundJobQueue("critical-test", max_size=1, workers=1)
        gate = asyncio.Event()
        executed: list[int] = []

        async def first():
            await gate.wait()
            executed.append(1)

        async def second():
            executed.append(2)

        self.assertTrue(await queue.submit(first))
        submit_task = asyncio.create_task(queue.submit(second, critical=True))
        await asyncio.sleep(0)
        self.assertFalse(submit_task.done())
        await queue.start()
        self.assertTrue(await submit_task)
        gate.set()
        await queue.stop(drain=True)
        self.assertEqual(executed, [1, 2])
        self.assertEqual(queue.dropped, 0)

    async def test_background_queue_retries_and_reports_metrics(self) -> None:
        metrics = RuntimeMetrics()
        queue = BackgroundJobQueue("retry-test", metrics=metrics)
        attempts = 0

        async def flaky():
            nonlocal attempts
            attempts += 1
            if attempts == 1:
                raise RuntimeError("temporary")

        await queue.start()
        self.assertTrue(await queue.submit(flaky, max_retries=1))
        await queue.stop(drain=True)
        self.assertEqual(attempts, 2)
        self.assertEqual(queue.retried, 1)
        self.assertEqual(queue.completed, 1)
        self.assertEqual(queue.failed, 0)
        self.assertEqual(queue.metrics_snapshot()["queue.retry-test.size"], 0)


if __name__ == "__main__":
    unittest.main()
