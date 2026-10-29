from __future__ import annotations

import tempfile
import unittest
from datetime import date, datetime
from pathlib import Path
from types import SimpleNamespace

import nonebot

nonebot.init()

from nonebot_plugin_xiuxian_2.features.activity.read_model_application import (
    ActivityReadModelApplication,
)
from nonebot_plugin_xiuxian_2.features.activity.read_model_repository import (
    ActivityReadModelSqlRepository,
)
from nonebot_plugin_xiuxian_2.features.activity.sign_history_view import (
    ActivitySignDayView,
    render_sign_calendar,
)
from nonebot_plugin_xiuxian_2.infrastructure.database import DatabaseUnitOfWork


class ActivitySignHistoryTests(unittest.TestCase):
    def setUp(self) -> None:
        self.temp = tempfile.TemporaryDirectory()
        self.database = Path(self.temp.name) / "game.db"
        with DatabaseUnitOfWork(self.database, immediate=True) as uow:
            uow.execute(
                "CREATE TABLE activity_sign_log("
                "id INTEGER PRIMARY KEY AUTOINCREMENT,user_id TEXT NOT NULL,"
                "sign_date TEXT NOT NULL,day_index INTEGER NOT NULL DEFAULT 0,"
                "reward TEXT DEFAULT '',milestone_reward TEXT DEFAULT '',"
                "reward_status TEXT DEFAULT '',reward_message TEXT DEFAULT '',"
                "create_time TEXT DEFAULT '',finish_time TEXT DEFAULT '',"
                "UNIQUE(user_id,sign_date))"
            )
        self.repository = ActivityReadModelSqlRepository(self.database)

    def tearDown(self) -> None:
        self.temp.cleanup()

    def insert(
        self,
        user_id: str,
        sign_date: str,
        *,
        day_index: int = 1,
        reward_status: str = "success",
    ) -> None:
        with DatabaseUnitOfWork(self.database, immediate=True) as uow:
            uow.execute(
                "INSERT INTO activity_sign_log("
                "user_id,sign_date,day_index,reward_status) VALUES(?,?,?,?)",
                (user_id, sign_date, day_index, reward_status),
            )

    def test_recent_signs_order_limit_and_user_scope(self) -> None:
        self.insert("u1", "2026-10-20")
        self.insert("u1", "2026-10-29")
        self.insert("u1", "2026-10-27")
        self.insert("u2", "2026-10-30")

        rows = self.repository.recent_signs("u1", limit=2)

        self.assertEqual([row.sign_date for row in rows], ["2026-10-29", "2026-10-27"])
        self.assertEqual((), self.repository.recent_signs("u1", limit=0))

    def test_missing_table_and_database_return_empty_without_ddl(self) -> None:
        empty = Path(self.temp.name) / "empty.db"
        with DatabaseUnitOfWork(empty, immediate=True) as uow:
            uow.execute("CREATE TABLE filler (id INTEGER)")
        missing = Path(self.temp.name) / "missing.db"

        self.assertEqual((), ActivityReadModelSqlRepository(empty).recent_signs("u1"))
        self.assertEqual((), ActivityReadModelSqlRepository(missing).recent_signs("u1"))
        self.assertFalse(missing.exists())
        with DatabaseUnitOfWork(empty) as uow:
            self.assertIsNone(
                uow.query_one(
                    "SELECT 1 FROM sqlite_master WHERE type='table' "
                    "AND name='activity_sign_log'"
                )
            )

    def test_renderer_fills_seven_day_window_and_skips_bad_dates(self) -> None:
        rows = (
            ActivitySignDayView("2026-10-29"),
            ActivitySignDayView("2026-10-27"),
            ActivitySignDayView("not-a-date"),
        )

        text = render_sign_calendar(rows, today=date(2026, 10, 29))
        lines = text.splitlines()

        self.assertIn("【最近活动签到】", text)
        self.assertEqual(len([line for line in lines if line[:5].count("-") == 1]), 7)
        self.assertIn("10-23 未签", text)
        self.assertIn("10-27 已签", text)
        self.assertIn("10-28 未签", text)
        self.assertIn("10-29 已签", text)
        self.assertEqual("", render_sign_calendar((), today=date(2026, 10, 29)))
        self.assertEqual(
            "",
            render_sign_calendar(
                (ActivitySignDayView("bad"),), today=date(2026, 10, 29)
            ),
        )

    def test_center_appends_sign_calendar_only_for_user_with_rows(self) -> None:
        config = {
            "name": "活动",
            "start_time": "2026-10-01",
            "end_time": "2026-10-31",
            "daily_tasks": [],
            "weekly_tasks": [],
            "extensions": {"activity_pass": {"enabled": False}},
        }
        app = ActivityReadModelApplication(
            "/tmp/activity-sign-history-missing.db",
            config_loader=lambda: config,
            clock=SimpleNamespace(now=lambda: datetime(2026, 10, 29, 12)),
        )
        app.repository.recent_signs = lambda user_id, limit=7: (
            ActivitySignDayView("2026-10-29"),
        )

        self.assertIn("【最近活动签到】", app.activity_center_text("u1"))
        self.assertIn("10-29 已签", app.activity_center_text("u1"))
        self.assertNotIn("【最近活动签到】", app.activity_center_text())

    def test_center_source_contract_reads_signs(self) -> None:
        source = (
            Path(__file__).resolve().parents[1]
            / "nonebot_plugin_xiuxian_2"
            / "features"
            / "activity"
            / "read_model_application.py"
        ).read_text(encoding="utf-8")
        start = source.index("def activity_center_text(")
        block = source[start : source.index("def activity_stage_text", start)]

        self.assertIn("recent_signs", block)
        self.assertIn("render_sign_calendar", block)


if __name__ == "__main__":
    unittest.main()
