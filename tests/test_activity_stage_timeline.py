"""Full stage-timeline view for 活动阶段.

The stage view previously rendered only the current and next stage; the
timeline section now lists every configured stage with its window, status and
— for a resolved player — the personal claim state of ended stages. Written as
``unittest.TestCase`` because the CI quality job collects tests with
``python -m unittest discover``.
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
    "key": "opened",
    "name": "开启期",
    "start_time": "2026-09-01",
    "end_time": "2026-10-01",
    "features": ["sign"],
    "rewards": [{"key": "stage", "name": "开启礼", "reward": "灵石x100"}],
}
ACTIVE = {
    "key": "running",
    "name": "进行期",
    "start_time": "2026-10-10",
    "end_time": "2026-10-20",
    "features": ["sign", "claim"],
}
FUTURE = {
    "key": "coming",
    "name": "冲刺期",
    "start_time": "2026-10-21",
    "end_time": "2026-10-31",
    "features": ["claim"],
    "rewards": [{"key": "sprint", "name": "冲刺礼", "reward": "灵石x200"}],
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


class ActivityStageTimelineTests(unittest.TestCase):
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

    def test_timeline_lists_every_stage_in_config_order(self) -> None:
        text = self._application([ENDED, ACTIVE, FUTURE]).activity_stage_text()
        self.assertIn("【阶段日程】", text)
        schedule = text.split("【阶段日程】")[1]
        opened = schedule.index("开启期")
        running = schedule.index("进行期")
        coming = schedule.index("冲刺期")
        self.assertLess(opened, running)
        self.assertLess(running, coming)
        self.assertIn("（已结束）", schedule)
        self.assertIn("（进行中）", schedule)
        self.assertIn("（未开始）", schedule)

    def test_personal_claim_marks_use_participation_evidence(self) -> None:
        with DatabaseUnitOfWork(self.database, immediate=True) as uow:
            uow.execute(
                "INSERT INTO activity_sign_log(user_id,sign_date,create_time) "
                "VALUES('u1','2026-09-20','2026-09-20 10:00:00')"
            )
        text = self._application([ENDED, ACTIVE]).activity_stage_text("u1")
        self.assertIn("奖励：可领取｜活动阶段领取", text)
        # Another player without participation evidence sees 未领取.
        other = self._application([ENDED, ACTIVE]).activity_stage_text("u2")
        self.assertIn("奖励：未领取", other)

    def test_claimed_stage_is_marked_claimed(self) -> None:
        with DatabaseUnitOfWork(self.database, immediate=True) as uow:
            uow.execute(
                "INSERT INTO activity_sign_log(user_id,sign_date,create_time) "
                "VALUES('u1','2026-09-20','2026-09-20 10:00:00')"
            )
            uow.execute(
                "INSERT INTO activity_stage_claim(activity_key,user_id,stage_key,reward,create_time) "
                "VALUES('spring','u1','opened','灵石x100','2026-10-02 00:00:00')"
            )
        text = self._application([ENDED]).activity_stage_text("u1")
        self.assertIn("奖励：已领取", text)
        self.assertNotIn("奖励：可领取", text)

    def test_anonymous_view_has_no_claim_marks(self) -> None:
        text = self._application([ENDED, ACTIVE]).activity_stage_text()
        self.assertIn("【阶段日程】", text)
        self.assertIn("奖励：已配置", text)
        self.assertNotIn("可领取｜活动阶段领取", text)
        self.assertNotIn("奖励：未领取", text)

    def test_single_stage_and_rewardless_stages_render(self) -> None:
        text = self._application([ACTIVE]).activity_stage_text()
        self.assertIn("【阶段日程】", text)
        self.assertIn("进行期（进行中）", text)
        # ACTIVE has no rewards: no claim marker at all.
        self.assertNotIn("奖励：", text.split("【阶段日程】")[1])

    def test_activity_closed_and_stageless_outputs_are_unchanged(self) -> None:
        closed = _config([ENDED])
        closed = {**closed, "enabled": False}
        text = ActivityReadModelApplication(
            self.database,
            config_loader=lambda: closed,
            clock=SimpleNamespace(now=lambda: NOW),
        ).activity_stage_text()
        self.assertNotIn("【阶段日程】", text)
        self.assertIn("活动未开启", text)
        stageless = self._application([]).activity_stage_text()
        self.assertNotIn("【阶段日程】", stageless)


if __name__ == "__main__":
    unittest.main()
