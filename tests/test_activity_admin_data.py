from __future__ import annotations

import tempfile
import unittest
import sqlite3
from contextlib import ExitStack
from pathlib import Path
from unittest.mock import patch

import nonebot

nonebot.init()

from nonebot_plugin_xiuxian_2.features.activity.admin_data_application import (
    ActivityAdminDataApplication,
)
from nonebot_plugin_xiuxian_2.features.activity.migrations import (
    apply_activity_event_receipts,
    apply_activity_state_schema,
)
from nonebot_plugin_xiuxian_2.infrastructure.database import DatabaseUnitOfWork
from nonebot_plugin_xiuxian_2.xiuxian.xiuxian_activity import (
    activity_config,
    activity_rules,
    service,
)
from nonebot_plugin_xiuxian_2.xiuxian.xiuxian_web import app
from nonebot_plugin_xiuxian_2.xiuxian.xiuxian_web import activity as web_activity
from nonebot_plugin_xiuxian_2.xiuxian.xiuxian_web import core as web_core


class ActivityAdminDataTests(unittest.TestCase):
    def setUp(self) -> None:
        self.maxDiff = None
        self.temp = tempfile.TemporaryDirectory()
        self.database = Path(self.temp.name) / "game.db"
        self.config = activity_config._migrate_config(
            activity_config._load_default_config()
        )[0]
        self.config["template_key"] = "festival_sign"
        self.config["start_time"] = "2026-10-01 00:00:00"
        self.config["gameplay_activities"] = [
            {"key": "points", "name": "积分活动", "type": "event_points", "enabled": True},
            {"key": "words", "name": "集字活动", "type": "collect_words", "enabled": True},
            {"key": "boss", "name": "首领活动", "type": "activity_boss", "enabled": True},
        ]
        self.config["daily_tasks"] = [
            {"key": "daily_sign", "name": "每日签到", "target": 1, "events": ["sign_in"]}
        ]
        self.config["weekly_tasks"] = []
        self.config.setdefault("extensions", {})["activity_pass"] = {
            "enabled": True,
            "level_exp": 100,
            "max_level": 12,
            "catchup_enabled": True,
            "catchup_start_day": 1,
            "catchup_level_gap": 1,
            "catchup_multiplier": 1.5,
        }

        with DatabaseUnitOfWork(self.database) as uow:
            apply_activity_state_schema(uow)
            apply_activity_event_receipts(uow)
            uow.execute(
                "CREATE TABLE user_xiuxian(user_id TEXT PRIMARY KEY,user_name TEXT)"
            )
            uow.execute(
                "INSERT INTO user_xiuxian(user_id,user_name) VALUES('user-0001','測試修士')"
            )
            uow.execute(
                "INSERT INTO activity_user(user_id,sign_days,total_sign_days,last_sign_date) "
                "VALUES('user-0001',4,9,'2026-10-05')"
            )
            uow.execute(
                "INSERT INTO activity_sign_log(user_id,sign_date,day_index) VALUES('user-0001','2026-10-06',4)"
            )
            uow.execute(
                "INSERT INTO activity_point_balance(activity_key,user_id,points,total_points) "
                "VALUES('points','user-0001',80,120)"
            )
            uow.execute(
                "INSERT INTO activity_point_purchase(activity_key,user_id,item_key,count) "
                "VALUES('points','user-0001','potion',2)"
            )
            uow.execute(
                "INSERT INTO activity_collect_inventory(activity_key,user_id,word_char,count) "
                "VALUES('words','user-0001','甲',3)"
            )
            uow.execute(
                "INSERT INTO activity_collect_claim(activity_key,user_id,phrase,count) "
                "VALUES('words','user-0001','甲乙',1)"
            )
            uow.execute(
                "INSERT INTO activity_collect_pity_state(activity_key,user_id,event_key,miss_count) "
                "VALUES('words','user-0001','work',5)"
            )
            uow.execute(
                "INSERT INTO activity_collect_drop_log(activity_key,user_id,event_key,word_char,create_time) "
                "VALUES('words','user-0001','work','甲','2026-10-06 10:00:00')"
            )
            uow.execute(
                "INSERT INTO activity_boss_state(activity_key,hp_left,max_hp) VALUES('boss',800,1000)"
            )
            uow.execute(
                "INSERT INTO activity_boss_damage(activity_key,user_id,total_damage) "
                "VALUES('boss','user-0001',200)"
            )
            uow.execute(
                "INSERT INTO activity_item_inventory(activity_key,user_id,item_id,count) "
                "VALUES('boss','user-0001','firework',2)"
            )
            uow.execute(
                "INSERT INTO activity_boss_fight_log(activity_key,user_id,damage,fight_date,source) "
                "VALUES('boss','user-0001',200,'2026-10-06','item')"
            )
            uow.execute(
                "INSERT INTO activity_boss_milestone(activity_key,milestone_key) VALUES('boss','one')"
            )
            task = activity_rules.get_activity_tasks(self.config)[0]
            uow.execute(
                "INSERT INTO activity_task_progress(activity_key,user_id,scope_type,scope_key,task_key,"
                "progress,target,claimed) VALUES(?,?,?,?,?,?,?,0)",
                (
                    activity_config._activity_config_key(self.config),
                    "user-0001",
                    "daily",
                    activity_rules._task_scope_key("daily"),
                    task["key"],
                    1,
                    1,
                ),
            )
            uow.execute(
                "INSERT INTO activity_pass_balance(activity_key,user_id,exp,total_exp,level) "
                "VALUES(?, 'user-0001',50,250,2)",
                (activity_config._activity_config_key(self.config),),
            )
            uow.execute(
                "INSERT INTO activity_pass_reward_claim(activity_key,user_id,level) VALUES(?, 'user-0001',1)",
                (activity_config._activity_config_key(self.config),),
            )

        self.application = ActivityAdminDataApplication(
            self.database,
            config_loader=lambda: self.config,
            today_provider=lambda: "2026-10-06",
            timestamp_provider=lambda: "2026-10-06 12:00:00",
        )

    def tearDown(self) -> None:
        self.temp.cleanup()

    def _config_patches(self):
        return (
            patch.object(activity_config, "activity_state", return_value=(True, "")),
            patch.object(
                activity_config,
                "activity_runtime_state",
                return_value={"ok": True, "features": ["sign", "task", "pass", "points", "collect", "boss"]},
            ),
            patch.object(service, "activity_state", return_value=(True, "")),
            patch.object(
                service,
                "activity_runtime_state",
                return_value={"ok": True, "features": ["sign", "task", "pass", "points", "collect", "boss"]},
            ),
            patch.object(activity_rules, "get_gameplay_activities", return_value=self.config["gameplay_activities"]),
            patch.object(service, "get_gameplay_activities", return_value=self.config["gameplay_activities"]),
        )

    def test_overview_matches_legacy_http_projection(self) -> None:
        fallbacks = {"user-0001": "測試修士"}
        with ExitStack() as stack:
            for context in self._config_patches():
                stack.enter_context(context)
            stack.enter_context(patch.object(service, "load_config", return_value=self.config))
            stack.enter_context(patch.object(service, "DB_PATH", self.database))
            stack.enter_context(patch.object(service, "today_str", return_value="2026-10-06"))
            stack.enter_context(
                patch.object(
                    service,
                    "_attach_display_names",
                    side_effect=lambda rows, id_key="user_id": [
                        {
                            **row,
                            "user_name": fallbacks.get(str(row.get(id_key) or ""), ""),
                            "display_name": fallbacks.get(str(row.get(id_key) or ""), ""),
                        }
                        for row in rows
                    ],
                )
            )
            legacy = service.get_activity_data_overview(user_id="user-0001", limit=10)
            migrated = self.application.overview(user_id="user-0001", limit=10)

        self.assertEqual(legacy, migrated)
        self.assertEqual("測試修士", migrated["sign_rank"][0]["display_name"])
        self.assertEqual(120, migrated["activities"][0]["user"]["balance"]["total_points"])
        self.assertEqual(2, migrated["activity_pass"]["user"]["level"])

    def test_reset_is_atomic_and_preserves_each_scope(self) -> None:
        message = self.application.reset("activity", "words")
        self.assertIn("影响记录 4 条", message)
        with DatabaseUnitOfWork(self.database, read_only=True) as uow:
            self.assertEqual(
                0,
                uow.query_one(
                    "SELECT COUNT(*) AS count FROM activity_collect_inventory WHERE activity_key='words'"
                )["count"],
            )
            self.assertEqual(
                80,
                uow.query_one(
                    "SELECT points FROM activity_point_balance WHERE activity_key='points' AND user_id='user-0001'"
                )["points"],
            )
            self.assertEqual(
                1,
                uow.query_one("SELECT COUNT(*) AS count FROM activity_user")["count"],
            )

        with self.assertRaisesRegex(ValueError, "请选择要清空"):
            self.application.reset("activity", "")
        with self.assertRaisesRegex(ValueError, "清理范围无效"):
            self.application.reset("unknown", "")

    def test_overview_uses_one_read_only_snapshot_and_does_not_create_database(self) -> None:
        units = []
        statements = []

        class TracedUnitOfWork(DatabaseUnitOfWork):
            def __enter__(inner_self):
                result = super().__enter__()
                units.append(inner_self)
                result.connection.set_trace_callback(statements.append)
                return result

        with ExitStack() as stack:
            for context in self._config_patches()[:2]:
                stack.enter_context(context)
            stack.enter_context(
                patch(
                    "nonebot_plugin_xiuxian_2.features.activity.admin_data_repository.DatabaseUnitOfWork",
                    TracedUnitOfWork,
                )
            )
            self.application.overview(user_id="user-0001")

        writes = [
            sql for sql in statements
            if sql.lstrip().lower().startswith(("insert", "update", "delete", "create", "drop", "alter"))
        ]
        self.assertEqual(1, len(units))
        self.assertTrue(units[0].read_only)
        self.assertEqual([], writes)

        missing_database = Path(self.temp.name) / "missing.db"
        missing_application = ActivityAdminDataApplication(
            missing_database,
            config_loader=lambda: self.config,
            today_provider=lambda: "2026-10-06",
        )
        with self.assertRaisesRegex(sqlite3.OperationalError, "unable to open database file"):
            missing_application.overview()
        self.assertFalse(missing_database.exists())

    def test_adjustments_preserve_clamping_and_validation(self) -> None:
        points = self.application.adjust(
            adjust_type="points", activity_key="points", user_id="user-0001",
            word_char="", amount=-200,
        )
        word = self.application.adjust(
            adjust_type="word", activity_key="words", user_id="user-0001",
            word_char="甲乙", amount=-5,
        )
        pass_exp = self.application.adjust(
            adjust_type="pass_exp", activity_key=None, user_id="user-0001",
            word_char=None, amount=100,
        )

        self.assertEqual({"points": 0, "total_points": 120}, points)
        self.assertEqual({"word_char": "甲", "count": 0}, word)
        self.assertEqual(
            {"exp": 50, "total_exp": 350, "level": 3, "level_exp": 100, "max_level": 12},
            pass_exp,
        )
        with self.assertRaisesRegex(ValueError, "请输入用户ID"):
            self.application.adjust(
                adjust_type="points", activity_key="points", user_id="", word_char="", amount=1
            )
        with self.assertRaisesRegex(ValueError, "请选择积分活动"):
            self.application.adjust(
                adjust_type="points", activity_key="words", user_id="user-0001", word_char="", amount=1
            )

    def test_web_routes_use_application_and_keep_csrf_boundary(self) -> None:
        app.config.update(TESTING=True, SECRET_KEY="activity-admin-data-test")
        client = app.test_client()
        with client.session_transaction() as session:
            session["admin_id"] = "admin-1"
            session["_csrf_token"] = "csrf-token"

        with (
            patch.object(web_core, "ADMIN_IDS", {"admin-1"}),
            patch.object(web_activity, "_activity_admin_data_application", return_value=self.application) as app_factory,
        ):
            overview = client.get("/api/activity/data?user_id=user-0001")
            missing_csrf = client.post(
                "/api/activity/data/reset",
                json={"scope": "activity", "activity_key": "words"},
            )
            adjusted = client.post(
                "/api/activity/data/adjust",
                json={"type": "points", "activity_key": "points", "user_id": "user-0001", "amount": 5},
                headers={"X-CSRF-Token": "csrf-token"},
            )
            reset = client.post(
                "/api/activity/data/reset",
                json={"scope": "activity", "activity_key": "words"},
                headers={"X-CSRF-Token": "csrf-token"},
            )

        self.assertEqual(200, overview.status_code, overview.get_json())
        self.assertIn("data", overview.get_json(), overview.get_json())
        self.assertEqual("測試修士", overview.get_json()["data"]["sign_rank"][0]["display_name"])
        self.assertEqual(403, missing_csrf.status_code)
        self.assertEqual(200, adjusted.status_code)
        self.assertTrue(adjusted.get_json()["success"])
        self.assertEqual(200, reset.status_code)
        self.assertTrue(reset.get_json()["success"])
        self.assertEqual(3, app_factory.call_count)


if __name__ == "__main__":
    unittest.main()
