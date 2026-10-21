"""Stage-claim guidance on the stage timeline.

The timeline marks each ended stage with its reward claim state; the
未领取 (no participation evidence) marker now points players at where
participation evidence comes from, and the 可领取 mark relies on the
verified claim semantics (no query = claim every claimable stage).
Written as ``unittest.TestCase`` because the CI quality job collects
tests with ``python -m unittest discover``.
"""

from __future__ import annotations

from datetime import datetime
import tempfile
import unittest
from pathlib import Path
from types import SimpleNamespace

import nonebot

nonebot.init()

from nonebot_plugin_xiuxian_2.features.activity.migrations import (
    apply_activity_state_schema,
)
from nonebot_plugin_xiuxian_2.features.activity.read_model_application import (
    ActivityReadModelApplication,
)
from nonebot_plugin_xiuxian_2.infrastructure.database import DatabaseUnitOfWork


NOW = datetime(2026, 10, 15, 12)

ENDED = {
    "key": "closed",
    "name": "攻坚期",
    "start_time": "2026-09-01",
    "end_time": "2026-10-01",
    "features": ["sign"],
    "rewards": [{"key": "stage", "name": "阶段礼", "reward": "灵石x100"}],
}
# 活动阶段领取的 runtime gate 使用真实时钟（_now_dt），进行期阶段窗口必须
# 永续，否则 CI 在固定窗口外运行时整个 gate 拒绝。
RUNNING = {
    "key": "running",
    "name": "进行期",
    "start_time": "0",
    "end_time": "无限",
    "features": ["sign", "claim"],
}


def _config(stages: list[dict]) -> dict:
    return {
        "template_key": "spring",
        "enabled": True,
        "name": "春日庆典",
        "start_time": "0",
        "end_time": "无限",
        "daily_tasks": [],
        "weekly_tasks": [],
        "extensions": {
            "activity_pass": {"enabled": False},
            "activity_info_mode": "full",
            "stages": stages,
        },
    }


class StageClaimGuidanceTests(unittest.TestCase):
    def setUp(self) -> None:
        tmp = tempfile.TemporaryDirectory()
        self.addCleanup(tmp.cleanup)
        self.database = Path(tmp.name) / "game.db"
        with DatabaseUnitOfWork(self.database, immediate=True) as uow:
            apply_activity_state_schema(uow)

    def _application(self, stages: list[dict]) -> ActivityReadModelApplication:
        return ActivityReadModelApplication(
            self.database,
            config_loader=lambda: _config(stages),
            clock=SimpleNamespace(now=lambda: NOW),
        )

    def test_unclaimed_stage_points_at_participation_sources(self) -> None:
        text = self._application([ENDED]).activity_stage_text("u1")
        self.assertIn(
            "奖励：未领取（参与记录来自活动期间的签到/任务/玩法记录）", text
        )

    def test_claimable_mark_unchanged_for_participant(self) -> None:
        with DatabaseUnitOfWork(self.database, immediate=True) as uow:
            uow.execute(
                "INSERT INTO activity_sign_log(user_id,sign_date,create_time) "
                "VALUES('u1','2026-09-20','2026-09-20 10:00:00')"
            )
        text = self._application([ENDED]).activity_stage_text("u1")
        self.assertIn("奖励：可领取｜活动阶段领取", text)
        self.assertNotIn("未领取", text)

    def test_claimed_mark_unchanged(self) -> None:
        with DatabaseUnitOfWork(self.database, immediate=True) as uow:
            uow.execute(
                "INSERT INTO activity_sign_log(user_id,sign_date,create_time) "
                "VALUES('u1','2026-09-20','2026-09-20 10:00:00')"
            )
            uow.execute(
                "INSERT INTO activity_stage_claim(activity_key,user_id,stage_key,reward,create_time) "
                "VALUES('spring','u1','closed','灵石x100','2026-10-02 00:00:00')"
            )
        text = self._application([ENDED]).activity_stage_text("u1")
        self.assertIn("奖励：已领取", text)
        self.assertNotIn("未领取", text)

    def test_anonymous_view_has_no_claim_marks(self) -> None:
        text = self._application([ENDED]).activity_stage_text()
        self.assertIn("奖励：已配置", text)
        self.assertNotIn("未领取", text)

    def test_claim_without_query_consumes_every_claimable_stage(self) -> None:
        """The no-query guidance is backed by real claim semantics."""
        from nonebot_plugin_xiuxian_2.features.activity.stage_claim_application import (
            ActivityStageClaimApplication,
        )

        with DatabaseUnitOfWork(self.database, immediate=True) as uow:
            uow.execute(
                "INSERT INTO activity_sign_log(user_id,sign_date,create_time) "
                "VALUES('u1','2026-09-20','2026-09-20 10:00:00')"
            )
        application = ActivityStageClaimApplication(
            self.database,
            clock=SimpleNamespace(now=lambda: NOW),
            config_loader=lambda: _config([ENDED, RUNNING]),
        )
        ok, message = application.claim("u1", query="")
        self.assertTrue(ok, message)
        # After the no-query claim the timeline flips to 已领取.
        text = self._application([ENDED]).activity_stage_text("u1")
        self.assertIn("奖励：已领取", text)
        self.assertNotIn("可领取", text)


if __name__ == "__main__":
    unittest.main()
