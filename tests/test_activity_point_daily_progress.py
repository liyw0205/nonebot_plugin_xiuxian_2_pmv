"""Point-event daily progress in the center progress line.

The settlement side caps each event rule's daily earnings by
``activity_point_event_log`` accumulation for the day; the center card now
renders that same accumulation as per-rule progress so the daily cap is no
longer a black box. ``points_text`` stays byte-identical with the legacy
``build_activity_points_text`` (parity-pinned), so the progress lives in the
center progress line only. Written as ``unittest.TestCase`` because the CI
quality job collects tests with ``python -m unittest discover``.
"""

from __future__ import annotations

from datetime import datetime
import tempfile
import unittest
from pathlib import Path
from types import SimpleNamespace

import nonebot

nonebot.init()

from nonebot_plugin_xiuxian_2.features.activity.gameplay_progress import (
    point_event_daily_parts,
)
from nonebot_plugin_xiuxian_2.features.activity.migrations import (
    apply_activity_state_schema,
)
from nonebot_plugin_xiuxian_2.features.activity.read_model_application import (
    ActivityReadModelApplication,
)
from nonebot_plugin_xiuxian_2.infrastructure.database import DatabaseUnitOfWork


NOW = datetime(2026, 10, 15, 12)

POINTS = {
    "key": "points",
    "type": "event_points",
    "enabled": True,
    "name": "积分活动",
    "start_time": "0",
    "end_time": "无限",
    "point_name": "活动积分",
    "event_rules": [
        {"event": "sign_in", "points": 5, "daily_limit": 20},
        {"event": "boss", "points": 10},
    ],
}


def _config() -> dict:
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
        },
        "gameplay_activities": [POINTS],
    }


class PointEventDailyProgressTests(unittest.TestCase):
    def setUp(self) -> None:
        tmp = tempfile.TemporaryDirectory()
        self.addCleanup(tmp.cleanup)
        self.database = Path(tmp.name) / "game.db"
        with DatabaseUnitOfWork(self.database, immediate=True) as uow:
            apply_activity_state_schema(uow)

    def _application(self) -> ActivityReadModelApplication:
        return ActivityReadModelApplication(
            self.database,
            config_loader=_config,
            clock=SimpleNamespace(now=lambda: NOW),
        )

    def _seed(self, rows: list[tuple[str, str, int, str]]) -> None:
        with DatabaseUnitOfWork(self.database, immediate=True) as uow:
            for event, user_id, points, date in rows:
                uow.execute(
                    "INSERT INTO activity_point_event_log"
                    "(activity_key,user_id,event_key,points,record_date,create_time) "
                    "VALUES('points',?,?,?,?,?)",
                    (user_id, event, points, date, f"{date} 10:00:00"),
                )
            uow.execute(
                "INSERT INTO activity_point_balance(activity_key,user_id,points,total_points) "
                "VALUES('points','u1',15,15)"
            )

    def test_center_progress_line_renders_daily_progress(self) -> None:
        self._seed([("sign_in", "u1", 15, "2026-10-15")])
        center = self._application().activity_center_text("u1")
        self.assertIn("活动积分 15（今日 修仙签到 今日 15/20）", center)

    def test_capped_rule_is_marked_as_reached(self) -> None:
        self._seed([("sign_in", "u1", 25, "2026-10-15")])
        center = self._application().activity_center_text("u1")
        self.assertIn("（今日 修仙签到 今日 20/20（已达上限））", center)

    def test_other_days_and_other_users_do_not_count(self) -> None:
        self._seed([
            ("sign_in", "u1", 25, "2026-10-14"),
            ("sign_in", "u2", 25, "2026-10-15"),
        ])
        center = self._application().activity_center_text("u1")
        self.assertIn("活动积分 15（今日 修仙签到 今日 0/20）", center)
        self.assertNotIn("已达上限", center)

    def test_anonymous_view_renders_no_progress(self) -> None:
        self._seed([("sign_in", "u1", 15, "2026-10-15")])
        center = self._application().activity_center_text("")
        self.assertNotIn("今日", center.split("进度：")[-1])

    def test_missing_event_log_table_keeps_the_balance_segment(self) -> None:
        self._seed([])
        with DatabaseUnitOfWork(self.database) as uow:
            uow.execute("DROP TABLE activity_point_event_log")
        center = self._application().activity_center_text("u1")
        self.assertIn("活动积分 15", center)
        self.assertNotIn("今日", center.split("进度：")[-1])

    def test_rules_without_daily_limit_render_no_progress(self) -> None:
        config = _config()
        activity = dict(POINTS, event_rules=[{"event": "boss", "points": 10}])
        config["gameplay_activities"] = [activity]
        with DatabaseUnitOfWork(self.database, immediate=True) as uow:
            uow.execute(
                "INSERT INTO activity_point_balance(activity_key,user_id,points,total_points) "
                "VALUES('points','u1',15,15)"
            )
        application = ActivityReadModelApplication(
            self.database,
            config_loader=lambda: config,
            clock=SimpleNamespace(now=lambda: NOW),
        )
        center = application.activity_center_text("u1")
        self.assertIn("活动积分 15", center)
        self.assertNotIn("今日", center.split("进度：")[-1])

    def test_shared_helper_parts(self) -> None:
        activity = {
            "key": "points",
            "event_rules": [
                {"event": "sign_in", "points": 5, "daily_limit": 20},
                {"event": "boss", "points": 10},
            ],
        }
        parts = point_event_daily_parts(activity, {("points", "sign_in"): 20})
        self.assertEqual(parts, ["修仙签到 今日 20/20（已达上限）"])
        self.assertEqual(
            point_event_daily_parts(activity, {}), ["修仙签到 今日 0/20"]
        )


if __name__ == "__main__":
    unittest.main()
