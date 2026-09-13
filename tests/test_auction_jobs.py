from __future__ import annotations

import unittest

import nonebot

nonebot.init()

from nonebot_plugin_xiuxian_2.xiuxian.xiuxian_trade.auction_jobs import (
    get_auction_job_failure_count,
    reset_auction_job_failure_counts,
    run_auction_job,
)
from nonebot_plugin_xiuxian_2.features.auction.jobs import settle
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

    def test_scheduled_slot_produces_stable_operation_id(self) -> None:
        calls = []

        class Application:
            def settle_active(self, **kwargs):
                calls.append(kwargs)
                return kwargs

        settle(Application(), scheduled_at="1700000000")
        settle(Application(), scheduled_at="1700000000")
        self.assertEqual(calls[0]["operation_id"], "auction-settle:1700000000")
        self.assertEqual(calls[0]["operation_id"], calls[1]["operation_id"])
        self.assertEqual(calls[0]["end_time"], 1700000000.0)

    def test_manual_job_uses_injected_clock_and_id_generator(self) -> None:
        calls = []

        class Clock:
            def now(self):
                from datetime import datetime, timezone

                return datetime(2026, 9, 12, 1, 2, 3, tzinfo=timezone.utc)

        class Ids:
            def new_id(self):
                return "fixed-id"

        class Application:
            def settle_active(self, **kwargs):
                calls.append(kwargs)
                return kwargs

        settle(Application(), clock=Clock(), ids=Ids())
        self.assertEqual(calls[0]["operation_id"], "auction-settle:fixed-id")
        self.assertEqual(calls[0]["end_time"], 1789174923.0)


if __name__ == "__main__":
    unittest.main()
