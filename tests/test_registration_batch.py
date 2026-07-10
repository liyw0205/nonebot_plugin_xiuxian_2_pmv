from __future__ import annotations

import asyncio
import unittest

import nonebot

nonebot.init()

from nonebot_plugin_xiuxian_2.xiuxian.xiuxian_base.registration_batch import (
    RegistrationBatcher,
    RegistrationRequest,
)


class FakeRegistrationManager:
    def __init__(self) -> None:
        self.calls = []

    def create_users_batch_fast(self, rows):
        rows = list(rows)
        self.calls.append(rows)
        return {
            row["request_id"]: (True, f"created:{row['user_id']}")
            for row in rows
        }


class RegistrationBatcherTests(unittest.IsolatedAsyncioTestCase):
    async def test_concurrent_submits_are_coalesced_into_one_batch(self) -> None:
        manager = FakeRegistrationManager()
        batcher = RegistrationBatcher(manager, max_batch_size=20, flush_delay=0.02)

        async def submit(index: int):
            return await batcher.submit(
                RegistrationRequest(
                    str(index), "金", "极品", 100, "now", f"道友{index}"
                )
            )

        results = await asyncio.gather(*(submit(index) for index in range(10)))

        self.assertEqual(len(manager.calls), 1)
        self.assertEqual(len(manager.calls[0]), 10)
        self.assertEqual(results[0], (True, "created:0"))


if __name__ == "__main__":
    unittest.main()
