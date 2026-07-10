from __future__ import annotations

import unittest

import nonebot

nonebot.init()

from nonebot_plugin_xiuxian_2.xiuxian.xiuxian_trade.auction_jobs import (
    get_auction_job_failure_count,
    reset_auction_job_failure_counts,
    run_auction_job,
)
from tests.test_db_backend import db_backend


class AuctionJobTests(unittest.IsolatedAsyncioTestCase):
    def setUp(self) -> None:
        reset_auction_job_failure_counts()

    async def test_successful_sync_and_async_jobs_return_results(self) -> None:
        async def async_operation():
            return "async-ok"

        self.assertEqual(await run_auction_job("sync", lambda: "sync-ok"), "sync-ok")
        self.assertEqual(await run_auction_job("async", async_operation), "async-ok")
        self.assertEqual(get_auction_job_failure_count("sync"), 0)

    async def test_database_failures_are_counted_and_reraised(self) -> None:
        def fail():
            raise db_backend.OperationalError("database unavailable")

        with self.assertRaises(db_backend.OperationalError):
            await run_auction_job("end_check", fail)

        self.assertEqual(get_auction_job_failure_count("end_check"), 1)
        self.assertEqual(get_auction_job_failure_count("end_check", "database"), 1)

    async def test_suppressed_startup_failure_is_still_counted(self) -> None:
        def fail():
            raise OSError("session unavailable")

        result = await run_auction_job("startup_reconcile", fail, suppress=True)

        self.assertIsNone(result)
        self.assertEqual(
            get_auction_job_failure_count("startup_reconcile", "filesystem"),
            1,
        )


if __name__ == "__main__":
    unittest.main()
