import asyncio
from datetime import datetime, timezone
import tempfile
import unittest
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import AsyncMock, patch

import nonebot

nonebot.init()

from nonebot_plugin_xiuxian_2.features.activity.application import ActivityApplication
from nonebot_plugin_xiuxian_2.features.activity.migrations import apply_activity_state_schema
from nonebot_plugin_xiuxian_2.features.activity.read_model_application import ActivityReadModelApplication
from nonebot_plugin_xiuxian_2.infrastructure.database import DatabaseUnitOfWork
from nonebot_plugin_xiuxian_2.xiuxian import xiuxian_activity as activity_module
from nonebot_plugin_xiuxian_2.xiuxian.xiuxian_activity.activity_config import load_config
from nonebot_plugin_xiuxian_2.xiuxian.xiuxian_activity.activity_rules import get_gameplay_activities


class TestActivityGameplayPersonal(unittest.TestCase):
    def setUp(self) -> None:
        self.temp = tempfile.TemporaryDirectory()
        self.database = Path(self.temp.name) / "game.db"
        self.config = load_config()
        with DatabaseUnitOfWork(self.database) as uow:
            apply_activity_state_schema(uow)
            uow.execute("INSERT INTO activity_user(user_id, sign_days, total_sign_days, last_sign_date) VALUES('user-1', 4, 9, '2026-10-05')")
        self.activities = {a["type"]: a for a in get_gameplay_activities(self.config)}
        self.collect = self.activities["collect_words"]
        self.points = self.activities["event_points"]
        self.boss = self.activities["activity_boss"]

    def tearDown(self) -> None:
        self.temp.cleanup()

    def _application(self) -> ActivityReadModelApplication:
        return ActivityReadModelApplication(
            self.database, config_loader=lambda: self.config
        )

    def test_overview_snapshot_one_call(self) -> None:
        application = self._application()
        point_rule = next(
            r for r in self.points["event_rules"]
            if r.get("daily_limit") and r.get("event") == "work"
        )
        now = datetime(2026, 10, 23, 12, tzinfo=timezone.utc)
        today = now.date().isoformat()
        with DatabaseUnitOfWork(self.database) as uow:
            uow.execute("INSERT INTO activity_collect_pity_state(activity_key,user_id,event_key,miss_count) VALUES(?,?,?,?)", (self.collect["key"], "user-1", self.collect["drop_events"][0], 5))
            uow.execute("INSERT INTO activity_collect_inventory(activity_key,user_id,word_char,count) VALUES(?,?,?,?)", (self.collect["key"], "user-1", "福", 2))
            uow.execute("INSERT INTO activity_point_balance(activity_key,user_id,points,total_points) VALUES(?,?,?,?)", (self.points["key"], "user-1", 35, 50))
            uow.execute("INSERT INTO activity_point_event_log(activity_key,user_id,event_key,points,record_date) VALUES(?,?,?,?,?)", (self.points["key"], "user-1", point_rule["event"], point_rule["daily_limit"], today))
            uow.execute("INSERT INTO activity_boss_state(activity_key,hp_left,max_hp) VALUES(?,?,?)", (self.boss["key"], 750, 1000))
            uow.execute("INSERT INTO activity_boss_damage(activity_key,user_id,total_damage) VALUES(?,?,?)", (self.boss["key"], "user-1", 1234))
            uow.execute("INSERT INTO activity_boss_fight_log(activity_key,user_id,damage,fight_date,source) VALUES(?,?,?,?,?)", (self.boss["key"], "user-1", 100, today, "coop"))
        with (
            patch.object(application.clock, "now", return_value=now),
            patch.object(application.repository, "overview_snapshot", wraps=application.repository.overview_snapshot) as snapshot,
        ):
            text = application.gameplay_personal_text("user-1")
        self.assertEqual(snapshot.call_count, 1)
        self.assertIn("福×2（共2张）", text)
        self.assertIn(f"保底 5/{self.collect['pity_threshold']}", text)
        self.assertIn(f"{self.points['name']} 庆典积分 35", text)
        self.assertIn(
            f"悬赏令 今日 {point_rule['daily_limit']}/{point_rule['daily_limit']}"
            "（已达上限）", text
        )
        self.assertIn("我的伤害 1234", text)
        self.assertIn(f"今日挑战 1/{self.boss['daily_fight_limit']}", text)

    def test_empty_state(self) -> None:
        config = dict(self.config)
        config["gameplay_activities"] = []
        application = ActivityReadModelApplication(
            self.database, config_loader=lambda: config
        )
        text = application.gameplay_personal_text("user-2")
        self.assertIn("暂无个人玩法进度", text)

    def test_parity_with_gameplay_text(self) -> None:
        application = self._application()
        text = application.gameplay_personal_text("user-1")
        self.assertTrue(text.startswith(application.gameplay_text()))

    def test_real_handler_wiring(self) -> None:
        source = (
            Path(__file__).resolve().parents[1]
            / "nonebot_plugin_xiuxian_2"
            / "xiuxian"
            / "xiuxian_activity"
            / "__init__.py"
        ).read_text(encoding="utf-8")
        start = source.index("@activity_gameplay_cmd.handle")
        handler_source = source[start : source.index("@activity_sign_cmd.handle", start)]
        self.assertIn('gameplay_personal_text(str(user_info["user_id"]))', handler_source)
        self.assertIn("else activity_application.read_model.gameplay_text()", handler_source)

        # Invoke the registered handler itself: a resolved user gets the
        # personal view, an unknown user falls back to the shared view.
        handler = activity_module.activity_gameplay_cmd.handlers[0].call
        application = ActivityApplication(self.database)
        event = SimpleNamespace(get_user_id=lambda: "user-1", message_id="m1")
        bot = SimpleNamespace(self_id="bot-1")
        sent: list[str] = []

        with (
            patch.object(activity_module, "assign_bot", AsyncMock(return_value=None)),
            patch.object(activity_module, "handle_send", AsyncMock(side_effect=lambda *a, **k: sent.append(str(k.get("message") or (a[-1] if a else ""))))),
            patch.object(activity_module, "_ensure_user", AsyncMock(return_value=(True, {"user_id": "user-1"}, ""))),
            patch.object(activity_module, "activity_application", application),
        ):
            asyncio.run(handler(bot, event))
        self.assertEqual(1, len(sent))
        self.assertTrue(sent[0].startswith(application.read_model.gameplay_text()))
        self.assertIn("【我的玩法进度】", sent[0])
        self.assertIn(f"{self.collect['name']} 字牌", sent[0])

        sent.clear()
        with (
            patch.object(activity_module, "assign_bot", AsyncMock(return_value=None)),
            patch.object(activity_module, "handle_send", AsyncMock(side_effect=lambda *a, **k: sent.append(str(k.get("message") or (a[-1] if a else ""))))),
            patch.object(activity_module, "_ensure_user", AsyncMock(return_value=(False, None, "not-user"))),
            patch.object(activity_module, "activity_application", application),
        ):
            asyncio.run(handler(bot, event))
        self.assertEqual(1, len(sent))
        self.assertEqual(application.read_model.gameplay_text(), sent[0])


if __name__ == "__main__":
    unittest.main()
