"""Claim-all runner wiring: the production map must cover stage and rank rewards.

Written as ``unittest.TestCase`` because the CI quality job collects tests with
``python -m unittest discover``.
"""

from __future__ import annotations

import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

import nonebot

nonebot.init()

from nonebot_plugin_xiuxian_2.compatibility.legacy_activity_claim_steps import (
    build_legacy_activity_claim_runners,
)
from nonebot_plugin_xiuxian_2.features.activity_reward.claim_all_repository import (
    ActivityClaimAllRepository,
)
from nonebot_plugin_xiuxian_2.xiuxian.xiuxian_activity.transaction_service import (
    ActivityClaimAllService,
)


class ActivityClaimAllRunnerTests(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.database = Path(self.tmp.name) / "activity.db"

    def tearDown(self):
        self.tmp.cleanup()

    def test_production_runner_map_matches_both_step_plans(self):
        runners = build_legacy_activity_claim_runners("u")

        self.assertEqual(ActivityClaimAllService.step_names(), tuple(runners))
        self.assertEqual(ActivityClaimAllRepository.step_names(), tuple(runners))
        self.assertEqual(
            ("tasks", "pass", "boss_milestone", "boss_rank", "stage", "rank"),
            tuple(runners),
        )
        self.assertTrue(callable(runners["stage"]))
        self.assertTrue(callable(runners["rank"]))

    def test_production_runner_map_reaches_stage_and_rank_claims(self):
        calls = []

        class _Recorder:
            def __init__(self, name):
                self.name = name

            def claim(self, user_id, *, query=""):
                calls.append((self.name, user_id))
                return True, f"{self.name} reward"

        def _miss(uid, operation_id=None):
            return False, "miss"

        with patch(
            "nonebot_plugin_xiuxian_2.features.activity.stage_claim_application.ActivityStageClaimApplication",
            lambda database: _Recorder("stage"),
        ), patch(
            "nonebot_plugin_xiuxian_2.features.activity.rank_claim_application.ActivityRankClaimApplication",
            lambda database: _Recorder("rank"),
        ), patch(
            "nonebot_plugin_xiuxian_2.xiuxian.xiuxian_activity.service.claim_activity_tasks",
            _miss,
        ), patch(
            "nonebot_plugin_xiuxian_2.xiuxian.xiuxian_activity.service.claim_activity_pass_rewards",
            _miss,
        ), patch(
            "nonebot_plugin_xiuxian_2.xiuxian.xiuxian_activity.activity_boss.claim_boss_milestone_reward",
            _miss,
        ), patch(
            "nonebot_plugin_xiuxian_2.xiuxian.xiuxian_activity.activity_boss.claim_boss_rank_reward",
            _miss,
        ):
            service = ActivityClaimAllService(self.database)
            result = service.run(
                "prod-op", "u", build_legacy_activity_claim_runners("u")
            )

        self.assertEqual("applied", result.status)
        self.assertEqual([("stage", "u"), ("rank", "u")], calls)
        self.assertIn("stage reward", result.text)
        self.assertIn("rank reward", result.text)


if __name__ == "__main__":
    unittest.main()
