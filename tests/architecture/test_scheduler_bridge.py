from __future__ import annotations

import unittest
from unittest.mock import Mock

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

    def test_partial_registration_failure_retains_pending_jobs_and_retries(self) -> None:
        scheduler = _FakeScheduler()
        bridge = DeferredScheduler(scheduler)
        for job_id in ("before", "demon_invasion_schedule", "after"):
            bridge.add_job(lambda: None, "cron", id=job_id)
        original = RuntimeError("isolated scheduler store unavailable")
        add_job = scheduler.add_job

        def fail_demon(function, *args, **kwargs):
            if kwargs["id"] == "demon_invasion_schedule":
                raise original
            return add_job(function, *args, **kwargs)

        scheduler.add_job = Mock(side_effect=fail_demon)
        for _ in range(2):
            with self.assertRaises(RuntimeError) as raised:
                bridge.activate()
            self.assertIs(raised.exception, original)
            self.assertFalse(bridge.active)
            self.assertEqual(bridge.registered_ids, ("before",))
            self.assertEqual(bridge.pending_ids, ("demon_invasion_schedule", "after"))
            self.assertEqual(set(scheduler.jobs), {"before"})

        bridge.add_job(lambda: None, "cron", id="added-during-recovery")
        self.assertNotIn("added-during-recovery", scheduler.jobs)
        scheduler.add_job.side_effect = add_job
        bridge.activate()
        self.assertTrue(bridge.active)
        self.assertEqual(bridge.pending_ids, ())
        self.assertEqual(set(scheduler.jobs), {"before", "demon_invasion_schedule", "after", "added-during-recovery"})
        ids = [call.kwargs["id"] for call in scheduler.add_job.call_args_list]
        self.assertEqual(ids.count("before"), 1)
        self.assertEqual(ids.count("demon_invasion_schedule"), 3)
        self.assertEqual(ids.count("after"), 1)
        bridge.activate()
        self.assertEqual(len(scheduler.add_job.call_args_list), len(ids))

    def test_existing_job_conflict_cannot_become_success_on_retry(self) -> None:
        scheduler = _FakeScheduler()
        existing = scheduler.add_job(lambda: None, "cron", id="demon_invasion_schedule")
        bridge = DeferredScheduler(scheduler)
        bridge.add_job(lambda: None, "cron", id="demon_invasion_schedule")
        for _ in range(2):
            with self.assertRaisesRegex(RuntimeError, "duplicate legacy scheduler job id: demon_invasion_schedule"):
                bridge.activate()
            self.assertFalse(bridge.active)
            self.assertEqual(bridge.pending_ids, ("demon_invasion_schedule",))
            self.assertIs(scheduler.get_job("demon_invasion_schedule"), existing)


if __name__ == "__main__":
    unittest.main()
