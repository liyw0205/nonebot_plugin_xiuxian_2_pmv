import tempfile
import unittest
from collections import OrderedDict
from pathlib import Path

import nonebot

nonebot.init()

from nonebot_plugin_xiuxian_2.xiuxian.xiuxian_activity.claim_all_service import (
    ActivityClaimAllService,
)
from tests.test_db_backend import db_backend


class _FailStepWriteOnceService(ActivityClaimAllService):
    def __init__(self, database, failed_step):
        super().__init__(database)
        self.failed_step = failed_step
        self.failed = False

    def _complete_step(self, operation_id, step_name, ok, text):
        if step_name == self.failed_step and not self.failed:
            self.failed = True
            raise RuntimeError("step write failed")
        return super()._complete_step(operation_id, step_name, ok, text)


class ActivityClaimAllTests(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.database = Path(self.tmp.name) / "activity.db"

    def tearDown(self):
        self.tmp.cleanup()

    @staticmethod
    def _runners(calls, outcomes):
        runners = OrderedDict()
        for step_name in ActivityClaimAllService.step_names():
            def run(operation_id, current=step_name):
                calls.append((current, operation_id))
                outcome = outcomes[current]
                if isinstance(outcome, Exception):
                    raise outcome
                return outcome

            runners[step_name] = run
        return runners

    def test_completes_fixed_plan_and_replays_exact_response(self):
        service = ActivityClaimAllService(self.database)
        calls = []
        runners = self._runners(
            calls,
            {
                "tasks": (True, "任务奖励"),
                "pass": (False, "战令暂无奖励"),
                "boss_milestone": (False, "进度暂无奖励"),
                "boss_rank": (True, "排行奖励"),
            },
        )

        result = service.run("claim-op", "u", runners)
        replay = service.run("claim-op", "u", runners)

        self.assertEqual("applied", result.status)
        self.assertTrue(result.ok)
        self.assertEqual("任务奖励\n\n排行奖励", result.text)
        self.assertEqual("duplicate", replay.status)
        self.assertEqual(result.text, replay.text)
        self.assertEqual(4, len(calls))
        self.assertEqual(
            [
                "claim-op:tasks",
                "claim-op:pass",
                "claim-op:boss-milestone",
                "claim-op:boss-rank",
            ],
            [operation_id for _, operation_id in calls],
        )

    def test_all_business_misses_are_completed(self):
        service = ActivityClaimAllService(self.database)
        calls = []
        runners = self._runners(
            calls,
            {step: (False, f"{step} miss") for step in service.step_names()},
        )

        result = service.run("miss-op", "u", runners)

        self.assertEqual("applied", result.status)
        self.assertFalse(result.ok)
        self.assertIn("暂无可领取奖励", result.text)
        with db_backend.connection(self.database) as conn:
            self.assertEqual(
                4,
                conn.execute(
                    "SELECT COUNT(*) FROM activity_claim_all_steps WHERE status='completed'"
                ).fetchone()[0],
            )

    def test_exception_keeps_step_retryable_and_resumes_in_order(self):
        service = ActivityClaimAllService(self.database)
        calls = []
        outcomes = {
            "tasks": (True, "task reward"),
            "pass": RuntimeError("temporary failure"),
            "boss_milestone": (False, "milestone miss"),
            "boss_rank": (False, "rank miss"),
        }
        runners = self._runners(calls, outcomes)

        first = service.run("retry-op", "u", runners)
        outcomes["pass"] = (True, "pass reward")
        second = service.run("retry-op", "u", runners)

        self.assertEqual("retryable_failure", first.status)
        self.assertEqual("applied", second.status)
        self.assertEqual(
            ["tasks", "pass", "pass", "boss_milestone", "boss_rank"],
            [step for step, _ in calls],
        )
        with db_backend.connection(self.database) as conn:
            attempts = dict(
                conn.execute(
                    "SELECT step_name,attempts FROM activity_claim_all_steps"
                ).fetchall()
            )
        self.assertEqual(1, attempts["tasks"])
        self.assertEqual(2, attempts["pass"])

    def test_recovers_child_result_after_step_progress_write_failure(self):
        service = _FailStepWriteOnceService(self.database, "pass")
        calls = []
        child_results = {}
        grants = []
        runners = OrderedDict()
        for step_name in service.step_names():
            def run(operation_id, current=step_name):
                calls.append(current)
                if operation_id not in child_results:
                    child_results[operation_id] = (True, f"{current} reward")
                    grants.append(current)
                return child_results[operation_id]

            runners[step_name] = run

        first = service.run("write-op", "u", runners)
        second = service.run("write-op", "u", runners)

        self.assertEqual("retryable_failure", first.status)
        self.assertEqual("applied", second.status)
        self.assertEqual(2, calls.count("pass"))
        self.assertEqual(1, grants.count("pass"))
        self.assertEqual(4, len(grants))

    def test_rejects_operation_reuse_for_another_user(self):
        service = ActivityClaimAllService(self.database)
        outcomes = {step: (False, "miss") for step in service.step_names()}
        service.run("conflict-op", "u1", self._runners([], outcomes))

        conflict = service.run("conflict-op", "u2", self._runners([], outcomes))

        self.assertEqual("operation_conflict", conflict.status)
        self.assertFalse(conflict.ok)

    def test_production_handler_uses_message_bound_claim_all_operation(self):
        activity_root = (
            Path(__file__).resolve().parents[1]
            / "nonebot_plugin_xiuxian_2"
            / "xiuxian"
            / "xiuxian_activity"
        )
        command_source = (activity_root / "__init__.py").read_text(encoding="utf-8")
        start = command_source.index("@activity_claim_cmd.handle")
        handler = command_source[start:command_source.index("@activity_tasks_cmd.handle", start)]
        self.assertIn('_activity_operation_id(event, "claim-all", user_id)', handler)

        service_source = (activity_root / "service.py").read_text(encoding="utf-8")
        start = service_source.index("def claim_activity_rewards")
        claim_all = service_source[start:service_source.index("def _parse_shop_query", start)]
        self.assertIn("activity_claim_all_service.run(", claim_all)
        self.assertNotIn("claim_boss_rewards(uid)", claim_all)


if __name__ == "__main__":
    unittest.main()
