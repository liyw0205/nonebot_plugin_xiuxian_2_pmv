"""Claim-all retry tells the player which steps already landed.

`docs/features/activity_reward.md` promises that a retryable failure keeps the
completed steps and a retry only runs the rest. The durable rows and the retry
semantics were already correct, but the player-facing text only named the step
that broke: a payer who saw "活动奖励领取未完成，请重试\n排行：boom" could not
tell that tasks/pass/boss rewards had already been granted, which reads as
"nothing happened" and invites duplicate support requests or manual
compensation.

Both coordinators carry the same plan (legacy `ActivityClaimAllService` and the
feature `ActivityClaimAllRepository`), so both retry messages now append
`已完成：<labels>` when earlier steps succeeded. Written as
``unittest.TestCase`` because the CI quality job collects with
``python -m unittest discover``.
"""

from __future__ import annotations

import tempfile
import unittest
from collections import OrderedDict
from pathlib import Path

import nonebot

nonebot.init()

from nonebot_plugin_xiuxian_2.features.activity_reward.claim_all_repository import (
    ActivityClaimAllRepository,
)
from nonebot_plugin_xiuxian_2.features.activity_reward.migrations import (
    apply_activity_claim_all,
)
from nonebot_plugin_xiuxian_2.infrastructure.database import DatabaseUnitOfWork
from nonebot_plugin_xiuxian_2.xiuxian.xiuxian_activity.transaction_service import (
    ActivityClaimAllService,
)
from tests.test_db_backend import db_backend


def _runners(step_names, outcomes, calls=None):
    runners = OrderedDict()
    for step_name in step_names:
        def run(operation_id, current=step_name):
            if calls is not None:
                calls.append(current)
            outcome = outcomes[current]
            if isinstance(outcome, Exception):
                raise outcome
            return outcome

        runners[step_name] = run
    return runners


def _all_ok(step_names):
    return {name: (True, f"{name} 领取成功") for name in step_names}


class LegacyClaimAllRetryMessageTests(unittest.TestCase):
    """旧协调器：中间步骤失败时回执要说明已完成的部分。"""

    def setUp(self) -> None:
        tmp = tempfile.TemporaryDirectory()
        self.addCleanup(tmp.cleanup)
        self.database = Path(tmp.name) / "activity.db"
        self.service = ActivityClaimAllService(self.database)

    def test_later_step_failure_lists_the_steps_already_applied(self) -> None:
        outcomes = _all_ok(self.service.step_names())
        outcomes["rank"] = RuntimeError("rank exploded")

        first = self.service.run("op-1", "u1", _runners(self.service.step_names(), outcomes))

        self.assertEqual("retryable_failure", first.status)
        self.assertIn("排行：rank exploded", first.text)
        self.assertIn("已发放：任务、战令、首领进度、首领排行、阶段", first.text)
        self.assertNotIn("已完成：任务、战令、首领进度、首领排行、阶段、排行", first.text)

    def test_first_step_failure_has_no_completed_suffix(self) -> None:
        outcomes = _all_ok(self.service.step_names())
        outcomes["tasks"] = RuntimeError("tasks exploded")

        first = self.service.run("op-2", "u1", _runners(self.service.step_names(), outcomes))

        self.assertEqual("retryable_failure", first.status)
        self.assertIn("任务：tasks exploded", first.text)
        self.assertNotIn("已完成：", first.text)

    def test_completed_suffix_follows_the_fixed_plan_order(self) -> None:
        """中途重试再次失败时，已完成段仍按固定六步顺序列出。"""
        outcomes = _all_ok(self.service.step_names())
        outcomes["boss_rank"] = RuntimeError("first boom")
        runners = _runners(self.service.step_names(), outcomes)

        self.service.run("op-3", "u1", runners)
        outcomes["boss_rank"] = RuntimeError("second boom")
        second = self.service.run("op-3", "u1", runners)

        self.assertEqual("retryable_failure", second.status)
        self.assertIn("已发放：任务、战令、首领进度", second.text)
        self.assertIn("首领排行：second boom", second.text)

    def test_retry_still_only_reruns_unfinished_steps(self) -> None:
        """回执文案变化不影响重试语义：已完成步骤不重复执行。"""
        outcomes = _all_ok(self.service.step_names())
        outcomes["stage"] = RuntimeError("stage boom")
        calls: list[str] = []
        runners = _runners(self.service.step_names(), outcomes, calls)

        first = self.service.run("op-4", "u1", runners)
        self.assertIn("已发放：任务、战令、首领进度、首领排行", first.text)
        applied_before = list(calls)

        outcomes["stage"] = (True, "stage 领取成功")
        calls.clear()
        second = self.service.run("op-4", "u1", runners)

        self.assertEqual("applied", second.status)
        self.assertEqual(applied_before[4:], ["stage"])
        self.assertEqual(["stage", "rank"], calls)


class FeatureClaimAllRetryMessageTests(unittest.TestCase):
    """feature 协调器：同一合同，同一回执文案规则。"""

    def setUp(self) -> None:
        tmp = tempfile.TemporaryDirectory()
        self.addCleanup(tmp.cleanup)
        self.database = Path(tmp.name) / "activity.db"
        with DatabaseUnitOfWork(self.database) as uow:
            apply_activity_claim_all(uow)
        self.repository = ActivityClaimAllRepository(self.database)

    def test_later_step_failure_lists_the_steps_already_applied(self) -> None:
        names = self.repository.step_names()
        outcomes = _all_ok(names)
        outcomes["rank"] = RuntimeError("rank exploded")

        first = self.repository.run("fop-1", "u1", _runners(names, outcomes))

        self.assertEqual("retryable_failure", first.status)
        self.assertIn("排行：rank exploded", first.text)
        self.assertIn("已发放：任务、战令、首领进度、首领排行、阶段", first.text)
        self.assertEqual(
            ["tasks", "pass", "boss_milestone", "boss_rank", "stage"],
            [step.name for step in first.steps],
        )

    def test_first_step_failure_has_no_completed_suffix(self) -> None:
        names = self.repository.step_names()
        outcomes = _all_ok(names)
        outcomes["tasks"] = RuntimeError("tasks exploded")

        first = self.repository.run("fop-2", "u1", _runners(names, outcomes))

        self.assertEqual("retryable_failure", first.status)
        self.assertNotIn("已完成：", first.text)

    def test_retry_resumes_and_finishes_without_duplicating_steps(self) -> None:
        names = self.repository.step_names()
        outcomes = _all_ok(names)
        outcomes["boss_milestone"] = RuntimeError("milestone boom")
        calls: list[str] = []
        runners = _runners(names, outcomes, calls)

        self.repository.run("fop-3", "u1", runners)
        outcomes["boss_milestone"] = (True, "boss_milestone 领取成功")
        calls.clear()
        second = self.repository.run("fop-3", "u1", runners)

        self.assertEqual("applied", second.status)
        self.assertEqual(["boss_milestone", "boss_rank", "stage", "rank"], calls)


class ClaimAllLabelOrderParityTests(unittest.TestCase):
    """两条路径的固定计划与标签必须一致，否则回执文案会漂移。"""

    def test_legacy_and_feature_plans_match(self) -> None:
        self.assertEqual(
            ActivityClaimAllService.step_names(),
            ActivityClaimAllRepository.step_names(),
        )
        self.assertEqual(
            ActivityClaimAllService.STEP_LABELS,
            ActivityClaimAllRepository.STEP_LABELS,
        )


if __name__ == "__main__":
    unittest.main()
