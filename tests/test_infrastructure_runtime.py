from __future__ import annotations

import asyncio
import unittest
from types import SimpleNamespace

import nonebot

nonebot.init()

from nonebot_plugin_xiuxian_2.xiuxian.infrastructure import (
    BackgroundJobQueue,
    QQEventDeduplicator,
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

        dedup = QQEventDeduplicator()
        bot = SimpleNamespace(self_id="bot-1")
        self.assertFalse(await dedup.is_duplicate(bot, Event()))
        self.assertTrue(await dedup.is_duplicate(bot, Event()))
        self.assertFalse(await dedup.is_duplicate(SimpleNamespace(self_id="bot-2"), Event()))

        event_without_id = Event()
        event_without_id.id = ""
        self.assertFalse(await dedup.is_duplicate(bot, event_without_id))
        self.assertFalse(await dedup.is_duplicate(bot, event_without_id))

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


if __name__ == "__main__":
    unittest.main()
