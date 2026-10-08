from __future__ import annotations

import unittest
from datetime import datetime, timezone
from types import SimpleNamespace
from unittest.mock import patch

import nonebot

from ..reward_application import WorkRewardApplication


nonebot.init()


class WorkRewardApplicationTests(unittest.TestCase):
    def test_generation_uses_supplied_profile_and_builds_stable_snapshot(self):
        app = WorkRewardApplication()
        clock = SimpleNamespace(now=lambda: datetime(2026, 10, 9, 12, tzinfo=timezone.utc))
        random_source = SimpleNamespace()
        with patch(
            "nonebot_plugin_xiuxian_2.xiuxian.xiuxian_work.workmake.workmake",
            return_value={"采药": [80, 12, 5, 101, "成功", "失败"]},
        ) as workmake:
            task_list, offer = app.generate_offer(
                user_id="u",
                user_level="筑基",
                exp=100,
                clock=clock,
                random_source=random_source,
            )

        workmake.assert_called_once_with("筑基", 100, "筑基", random_source=random_source)
        self.assertEqual(task_list[0][:5], ["采药", 80, 12, 5, 101])
        self.assertEqual(offer["task_order"], ["采药"])
        self.assertEqual(offer["tasks"]["采药"]["award"], 12)

    def test_capture_generation_draws_multiplier_after_offer(self):
        app = WorkRewardApplication()
        clock = SimpleNamespace(now=lambda: datetime(2026, 10, 9, 12, tzinfo=timezone.utc))
        draws = []

        class Random:
            def randint(self, low, high):
                draws.append((low, high))
                return 3

        with patch(
            "nonebot_plugin_xiuxian_2.xiuxian.xiuxian_work.workmake.workmake",
            return_value={"采药": [80, 12, 5, 101, "成功", "失败"]},
        ):
            task_list, offer, multiplier = app.generate_capture_offer(
                user_id="u",
                user_level="筑基",
                exp=100,
                clock=clock,
                random_source=Random(),
            )

        self.assertEqual((multiplier, draws), (3, [(2, 5)]))
        self.assertEqual((task_list[0][2], offer["tasks"]["采药"]["award"]), (36, 36))

    def test_settlement_decision_freezes_roll_and_big_success_multiplier(self):
        class Random:
            def randint(self, low, high):
                return 1

            def uniform(self, low, high):
                return 2.0

        decision = WorkRewardApplication().resolve_settlement(
            offer_snapshot={
                "status": 2,
                "tasks": {"采药": {"rate": 100, "award": 10, "item_id": 7}},
            },
            work_name="采药",
            random_source=Random(),
        )
        self.assertEqual(
            (decision.exp_gain, decision.success_kind, decision.item_id),
            (20, "big", 7),
        )


if __name__ == "__main__":
    unittest.main()
