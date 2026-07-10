from __future__ import annotations

import tempfile
import unittest
from pathlib import Path

import nonebot
from apscheduler.schedulers.background import BackgroundScheduler

nonebot.init()

from nonebot_plugin_xiuxian_2.xiuxian.xiuxian_scheduler.job_manager import (
    SchedulerJobManager,
)


def noop():
    return None


class SchedulerJobManagerTests(unittest.TestCase):
    def setUp(self) -> None:
        self.temp_dir = tempfile.TemporaryDirectory()
        self.store_path = Path(self.temp_dir.name) / "scheduler_overrides.json"
        self.scheduler = BackgroundScheduler(timezone="UTC")
        self.scheduler.add_job(noop, "cron", minute="5", id="cron-job")
        self.scheduler.add_job(noop, "interval", seconds=60, id="interval-job")
        self.scheduler.start(paused=True)
        self.manager = SchedulerJobManager(self.scheduler, self.store_path)

    def tearDown(self) -> None:
        self.scheduler.shutdown(wait=False)
        self.temp_dir.cleanup()

    def test_disable_and_schedule_override_are_persisted(self) -> None:
        disabled = self.manager.set_enabled("cron-job", False)
        updated = self.manager.reschedule(
            "interval-job", {"type": "interval", "seconds": 120}
        )

        self.assertFalse(disabled["enabled"])
        self.assertEqual(updated["trigger"], {"type": "interval", "seconds": 120})

        restart = BackgroundScheduler(timezone="UTC")
        restart.add_job(noop, "cron", minute="5", id="cron-job")
        restart.add_job(noop, "interval", seconds=60, id="interval-job")
        restart.start(paused=True)
        try:
            SchedulerJobManager(restart, self.store_path).apply_persisted_overrides()
            self.assertIsNone(restart.get_job("cron-job").next_run_time)
            self.assertEqual(
                int(restart.get_job("interval-job").trigger.interval.total_seconds()),
                120,
            )
        finally:
            restart.shutdown(wait=False)

    def test_manual_run_is_queued_without_enabling_paused_job(self) -> None:
        self.manager.set_enabled("cron-job", False)
        queued = self.manager.queue_manual_run("cron-job")

        self.assertTrue(queued["queued"])
        self.assertIsNone(self.scheduler.get_job("cron-job").next_run_time)
        self.assertIsNotNone(self.scheduler.get_job("web-manual:cron-job"))
        with self.assertRaises(ValueError):
            self.manager.queue_manual_run("cron-job")

    def test_reschedule_keeps_paused_job_disabled(self) -> None:
        self.manager.set_enabled("interval-job", False)

        updated = self.manager.reschedule(
            "interval-job", {"type": "interval", "seconds": 120}
        )

        self.assertFalse(updated["enabled"])
        self.assertIsNone(self.scheduler.get_job("interval-job").next_run_time)

    def test_invalid_trigger_is_rejected(self) -> None:
        with self.assertRaises(ValueError):
            self.manager.reschedule(
                "cron-job", {"type": "cron", "fields": {"unsupported": "*"}}
            )
