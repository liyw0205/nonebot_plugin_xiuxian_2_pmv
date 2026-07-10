from __future__ import annotations

import asyncio
import time
import unittest

import nonebot

nonebot.init()

from nonebot_plugin_xiuxian_2.xiuxian.xiuxian_entertainment.io_runtime import (
    EntertainmentIOTimeout,
    run_blocking_io,
    run_media_send,
)


class EntertainmentIOTests(unittest.IsolatedAsyncioTestCase):
    async def test_blocking_io_does_not_block_event_loop(self) -> None:
        marker = asyncio.Event()

        async def mark_after_yield() -> None:
            await asyncio.sleep(0.01)
            marker.set()

        io_task = asyncio.create_task(run_blocking_io(time.sleep, 0.08, timeout=1))
        marker_task = asyncio.create_task(mark_after_yield())
        await asyncio.sleep(0.03)

        self.assertTrue(marker.is_set())
        self.assertFalse(io_task.done())
        self.assertIsNone(await io_task)
        await marker_task

    async def test_blocking_io_timeout_is_reported(self) -> None:
        with self.assertRaisesRegex(EntertainmentIOTimeout, "网络操作超过"):
            await run_blocking_io(time.sleep, 0.08, timeout=0.01)

    async def test_media_send_timeout_is_reported(self) -> None:
        async def slow_send() -> None:
            await asyncio.sleep(0.08)

        with self.assertRaisesRegex(EntertainmentIOTimeout, "视频发送超过"):
            await run_media_send(slow_send, timeout=0.01, media_type="视频")

    async def test_media_send_has_global_concurrency_limit(self) -> None:
        active = 0
        peak = 0

        async def send() -> None:
            nonlocal active, peak
            active += 1
            peak = max(peak, active)
            await asyncio.sleep(0.03)
            active -= 1

        await asyncio.gather(
            *(run_media_send(send, timeout=1, media_type="图片") for _ in range(8))
        )

        self.assertLessEqual(peak, 3)


if __name__ == "__main__":
    unittest.main()
