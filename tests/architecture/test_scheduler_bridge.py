from __future__ import annotations

import unittest

from nonebot_plugin_xiuxian_2.compatibility.scheduler import DeferredScheduler


class _FakeScheduler:
    def __init__(self) -> None:
        self.jobs = {}

    def add_job(self, function, *args, **kwargs):
        job_id = kwargs.get("id")
        job = (function, args, kwargs)
        self.jobs[job_id] = job
        return job

    def get_job(self, job_id):
        return self.jobs.get(job_id)


class SchedulerBridgeTests(unittest.TestCase):
    def test_decorators_are_deferred_until_activation(self) -> None:
        scheduler = _FakeScheduler()
        bridge = DeferredScheduler(scheduler)

        @bridge.scheduled_job("cron", id="legacy-job")
        def job():
            return "ok"

        self.assertEqual(scheduler.jobs, {})
        self.assertEqual(bridge.pending_ids, ("legacy-job",))
        bridge.activate()
        self.assertIn("legacy-job", scheduler.jobs)
        self.assertEqual(bridge.registered_ids, ("legacy-job",))

    def test_activation_is_idempotent_and_late_additions_are_live(self) -> None:
        scheduler = _FakeScheduler()
        bridge = DeferredScheduler(scheduler)

        @bridge.scheduled_job("interval", id="one")
        def one():
            return None

        bridge.activate()
        bridge.activate()
        self.assertEqual(len(scheduler.jobs), 1)
        bridge.add_job(lambda: None, "interval", seconds=1, id="two")
        self.assertEqual(set(scheduler.jobs), {"one", "two"})

        @bridge.scheduled_job("interval", id="three")
        def three():
            return None

        self.assertEqual(bridge.pending_ids, ())
        self.assertEqual(set(scheduler.jobs), {"one", "two", "three"})

    def test_add_job_accepts_apscheduler_func_keyword(self) -> None:
        scheduler = _FakeScheduler()
        bridge = DeferredScheduler(scheduler)

        def generated():
            return None

        bridge.add_job(func=generated, trigger="interval", minutes=5, id="generated")
        self.assertEqual(scheduler.jobs, {})
        bridge.activate()
        self.assertIn("generated", scheduler.jobs)
        self.assertIs(scheduler.jobs["generated"][0], generated)


if __name__ == "__main__":
    unittest.main()
