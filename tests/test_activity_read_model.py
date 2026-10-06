from __future__ import annotations

import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

import nonebot

nonebot.init()

from nonebot_plugin_xiuxian_2.features.activity.migrations import (
    apply_activity_event_receipts,
    apply_activity_state_schema,
)
from nonebot_plugin_xiuxian_2.features.activity.read_model_application import (
    ActivityReadModelApplication,
)
from nonebot_plugin_xiuxian_2.infrastructure.database import DatabaseUnitOfWork
from nonebot_plugin_xiuxian_2.xiuxian.xiuxian_activity import (
    activity_boss,
    activity_config,
    service,
)
from nonebot_plugin_xiuxian_2.xiuxian.xiuxian_activity.activity_storage import (
    resolve_daohao_batch,
)


class ActivityReadModelTests(unittest.TestCase):
    def setUp(self) -> None:
        self.temp = tempfile.TemporaryDirectory()
        self.database = Path(self.temp.name) / "game.db"
        self.config = activity_config._migrate_config(
            activity_config._load_default_config()
        )[0]
        with DatabaseUnitOfWork(self.database) as uow:
            apply_activity_state_schema(uow)
            apply_activity_event_receipts(uow)
            uow.execute(
                "INSERT INTO activity_user(user_id,sign_days,total_sign_days,last_sign_date) "
                "VALUES('user-1',4,9,'2026-10-05')"
            )
            activity_key = activity_config._activity_config_key(self.config)
            task = service.get_activity_tasks(self.config, "daily")[0]
            uow.execute(
                "INSERT INTO activity_task_progress(activity_key,user_id,scope_type,scope_key,"
                "task_key,progress,target,claimed) VALUES(?,?,?,?,?,?,?,0)",
                (
                    activity_key,
                    "user-1",
                    "daily",
                    service._task_scope_key("daily"),
                    task["key"],
                    task["target"],
                    task["target"],
                ),
            )
            uow.execute(
                "INSERT INTO activity_pass_balance(activity_key,user_id,exp,total_exp,level) "
                "VALUES(?,?,0,100,1)",
                (activity_key, "user-1"),
            )

    def tearDown(self) -> None:
        self.temp.cleanup()

    def test_overview_reuses_one_connection_and_batched_user_progress(self) -> None:
        statements = []
        connections = []
        real_connect = service.db_backend.connect_readonly

        def connect(*args, **kwargs):
            conn = real_connect(*args, **kwargs)
            conn._raw.set_trace_callback(statements.append)
            connections.append(conn)
            return conn

        with (
            patch.object(service, "load_config", return_value=self.config),
            patch.object(service, "DB_PATH", self.database),
            patch.object(service.db_backend, "connect_readonly", side_effect=connect),
        ):
            text = service.build_activity_info("user-1")

        normalized = [statement.lower() for statement in statements]
        task_reads = [
            statement for statement in normalized
            if statement.lstrip().startswith("select")
            and "from activity_task_progress" in statement
        ]
        pass_balance_reads = [
            statement for statement in normalized
            if statement.lstrip().startswith("select")
            and "select exp, total_exp, level" in statement
        ]
        self.assertEqual(len(connections), 1)
        self.assertEqual(len(task_reads), 1)
        self.assertEqual(len(pass_balance_reads), 1)
        self.assertIn("我的累计签到：4 天", text)
        self.assertIn("1个任务奖励可领", text)
        self.assertIn("等级 1/12", text)

    def test_pass_summary_reads_balance_once_for_catchup(self) -> None:
        statements = []
        real_connect = service.db_backend.connect_readonly

        def connect(*args, **kwargs):
            conn = real_connect(*args, **kwargs)
            conn._raw.set_trace_callback(statements.append)
            return conn

        with (
            patch.object(service, "load_config", return_value=self.config),
            patch.object(service, "DB_PATH", self.database),
            patch.object(service.db_backend, "connect_readonly", side_effect=connect),
        ):
            text = service.build_activity_pass_text("user-1")

        balance_reads = [
            sql for sql in statements
            if "SELECT exp, total_exp, level" in sql
        ]
        self.assertEqual(len(balance_reads), 1)
        self.assertIn("等级：1/12", text)

    def test_read_model_application_preserves_task_and_pass_output(self) -> None:
        application = ActivityReadModelApplication(
            self.database,
            config_loader=lambda: self.config,
        )
        with (
            patch.object(service, "load_config", return_value=self.config),
            patch.object(service, "DB_PATH", self.database),
        ):
            self.assertEqual(
                application.task_progress_text("user-1"),
                service.build_activity_task_progress_text("user-1"),
            )
            self.assertEqual(
                application.task_catalog_text(),
                service.build_activity_tasks_text(),
            )
            self.assertEqual(
                application.pass_text("user-1"),
                service.build_activity_pass_text("user-1"),
            )

    def test_sign_rank_uses_one_read_only_join_and_preserves_display(self) -> None:
        with DatabaseUnitOfWork(self.database) as uow:
            uow.execute(
                "CREATE TABLE user_xiuxian(user_id TEXT PRIMARY KEY,user_name TEXT)"
            )
            uow.executemany(
                "INSERT INTO user_xiuxian(user_id,user_name) VALUES(?,?)",
                (("user-1", "甲"), ("user-2", "乙")),
            )
            uow.execute(
                "UPDATE activity_user SET sign_days=6,total_sign_days=6,last_sign_date=? "
                "WHERE user_id='user-1'",
                ("2026-10-05",),
            )
            uow.executemany(
                "INSERT INTO activity_user(user_id,sign_days,total_sign_days,last_sign_date) "
                "VALUES(?,?,?,?)",
                (
                    ("user-2", 6, 8, "2026-10-04"),
                    ("member-0003", 5, 7, "2026-10-03"),
                ),
            )
        statements = []
        from nonebot_plugin_xiuxian_2.features.activity.read_model_repository import (
            ActivityReadModelSqlRepository,
        )

        class TracedUnitOfWork(DatabaseUnitOfWork):
            def __enter__(inner_self):
                result = super().__enter__()
                result.connection.set_trace_callback(statements.append)
                return result

        application = ActivityReadModelApplication(
            self.database,
            repository=ActivityReadModelSqlRepository(self.database),
            config_loader=lambda: {"name": "测试活动"},
        )
        with patch(
            "nonebot_plugin_xiuxian_2.features.activity.read_model_repository.DatabaseUnitOfWork",
            TracedUnitOfWork,
        ):
            text = application.sign_rank_text()

        rank_reads = [
            sql for sql in statements
            if sql.lstrip().lower().startswith("select")
            and "left join user_xiuxian" in sql.lower()
        ]
        writes = [
            sql for sql in statements
            if sql.lstrip().lower().startswith(("insert", "update", "delete"))
        ]
        self.assertEqual(1, len(rank_reads))
        self.assertEqual([], writes)
        self.assertIn("【测试活动排行】", text)
        self.assertIn("1. 乙 累计签到 6 天", text)
        self.assertIn("2. 甲 累计签到 6 天", text)
        self.assertIn("3. 修士·0003 累计签到 5 天", text)

    def test_collect_bag_read_model_matches_legacy_text_and_uses_three_read_queries(self) -> None:
        from nonebot_plugin_xiuxian_2.features.activity.read_model_repository import (
            ActivityReadModelSqlRepository,
        )

        activity = next(
            activity
            for activity in service.get_gameplay_activities(self.config)
            if activity.get("type") == "collect_words"
        )
        phrases = service._collect_phrases(activity)
        letters = service._collect_letters(activity, phrases)
        with DatabaseUnitOfWork(self.database) as uow:
            for letter in letters:
                uow.execute(
                    "INSERT INTO activity_collect_inventory(activity_key,user_id,word_char,count) "
                    "VALUES(?,?,?,?)",
                    (activity["key"], "user-1", letter["char"], 2),
                )
            uow.execute(
                "INSERT INTO activity_collect_claim(activity_key,user_id,phrase,count) "
                "VALUES(?,?,?,?)",
                (activity["key"], "user-1", phrases[0]["phrase"], 2),
            )
            uow.execute(
                "INSERT INTO activity_collect_pity_state(activity_key,user_id,event_key,miss_count) "
                "VALUES(?,?,?,?)",
                (activity["key"], "user-1", "sign_in", 4),
            )

        statements = []
        units = []

        class TracedUnitOfWork(DatabaseUnitOfWork):
            def __enter__(inner_self):
                result = super().__enter__()
                units.append(inner_self)
                result.connection.set_trace_callback(statements.append)
                return result

        application = ActivityReadModelApplication(
            self.database,
            repository=ActivityReadModelSqlRepository(self.database),
            config_loader=lambda: self.config,
        )
        with (
            patch.object(service, "load_config", return_value=self.config),
            patch.object(service, "DB_PATH", self.database),
            patch(
                "nonebot_plugin_xiuxian_2.features.activity.read_model_repository.DatabaseUnitOfWork",
                TracedUnitOfWork,
            ),
        ):
            text = application.collect_bag_text("user-1")

        with (
            patch.object(service, "load_config", return_value=self.config),
            patch.object(service, "DB_PATH", self.database),
        ):
            legacy_text = service.build_collect_bag_text("user-1")

        collect_selects = [
            sql.lower()
            for sql in statements
            if sql.lstrip().lower().startswith("select")
            and any(
                table in sql.lower()
                for table in (
                    "activity_collect_inventory",
                    "activity_collect_claim",
                    "activity_collect_pity_state",
                )
            )
        ]
        writes = [
            sql.lower().lstrip()
            for sql in statements
            if sql.lower().lstrip().startswith(("insert", "update", "delete", "create", "drop"))
        ]
        self.assertEqual(legacy_text, text)
        self.assertEqual(1, len(units))
        self.assertTrue(units[0].read_only)
        self.assertEqual(3, len(collect_selects))
        self.assertEqual([], writes)

    def test_collect_bag_read_model_does_not_create_missing_database(self) -> None:
        from nonebot_plugin_xiuxian_2.features.activity.read_model_repository import (
            ActivityReadModelSqlRepository,
        )

        missing_database = Path(self.temp.name) / "missing-game.db"
        application = ActivityReadModelApplication(
            missing_database,
            repository=ActivityReadModelSqlRepository(missing_database),
            config_loader=lambda: self.config,
        )

        text = application.collect_bag_text("user-1")

        self.assertIn("【活动背包】", text)
        self.assertFalse(missing_database.exists())

    def test_limited_shop_stock_is_aggregated_per_activity(self) -> None:
        statements = []
        real_connect = service.db_backend.connect_readonly

        def connect(*args, **kwargs):
            conn = real_connect(*args, **kwargs)
            conn._raw.set_trace_callback(statements.append)
            return conn

        with (
            patch.object(service, "load_config", return_value=self.config),
            patch.object(service, "DB_PATH", self.database),
            patch.object(service.db_backend, "connect_readonly", side_effect=connect),
        ):
            text = service.build_activity_shop_text("user-1")

        stock_reads = [
            sql for sql in statements
            if "GROUP BY item_key" in sql
        ]
        self.assertEqual(len(stock_reads), 1)
        self.assertIn("全服库存", text)

    def test_boss_status_is_read_only_and_does_not_initialize_hp(self) -> None:
        activity = next(
            activity for activity in service.get_gameplay_activities(self.config)
            if activity.get("type") == "activity_boss"
        )
        statements = []
        real_connect = activity_boss.db_backend.connect_readonly

        def connect(*args, **kwargs):
            conn = real_connect(*args, **kwargs)
            conn._raw.set_trace_callback(statements.append)
            return conn

        with (
            patch.object(activity_boss, "load_config", return_value=self.config),
            patch.object(
                activity_boss, "get_gameplay_activities", return_value=[activity]
            ),
            patch.object(activity_boss, "DB_PATH", self.database),
            patch.object(
                activity_boss.db_backend, "connect_readonly", side_effect=connect
            ),
        ):
            text = activity_boss.build_boss_status_text(
                "user-1", activity["boss_name"]
            )

        with DatabaseUnitOfWork(self.database, read_only=True) as uow:
            state_count = uow.query_one(
                "SELECT COUNT(*) AS count FROM activity_boss_state"
            )["count"]
        writes = [
            statement.lower().lstrip()
            for statement in statements
            if statement.lower().lstrip().startswith(("insert", "update", "delete"))
        ]
        self.assertIn("全服血量", text)
        self.assertEqual(state_count, 0)
        self.assertEqual(writes, [])

    def test_boss_rank_uses_one_batched_display_name_lookup(self) -> None:
        activity = next(
            activity for activity in service.get_gameplay_activities(self.config)
            if activity.get("type") == "activity_boss"
        )
        with DatabaseUnitOfWork(self.database) as uow:
            uow.executemany(
                "INSERT INTO activity_boss_damage(activity_key,user_id,total_damage) "
                "VALUES(?,?,?)",
                ((activity["key"], "user-1", 20), (activity["key"], "user-2", 10)),
            )
        with (
            patch.object(activity_boss, "load_config", return_value=self.config),
            patch.object(
                activity_boss, "get_gameplay_activities", return_value=[activity]
            ),
            patch.object(activity_boss, "DB_PATH", self.database),
            patch.object(
                activity_boss,
                "resolve_daohao_batch",
                return_value={"user-1": "甲", "user-2": "乙"},
            ) as resolve_batch,
            patch.object(activity_boss, "resolve_daohao", side_effect=AssertionError),
        ):
            text = activity_boss.build_boss_rank_text(activity["boss_name"])

        resolve_batch.assert_called_once_with(["user-1", "user-2"])
        self.assertIn("甲 伤害 20", text)
        self.assertIn("乙 伤害 10", text)

    def test_batch_daohao_lookup_does_not_query_each_missing_user(self) -> None:
        class MessageStore:
            def __init__(self):
                self.calls = []

            def _read_query(self, sql, params, *, dict_row):
                self.calls.append((sql, params, dict_row))
                return [{"user_id": "user-1", "user_name": "甲"}]

        store = MessageStore()
        with patch(
            "nonebot_plugin_xiuxian_2.xiuxian.xiuxian_activity.activity_storage._sql_message",
            return_value=store,
        ):
            names = resolve_daohao_batch(["user-1", "user-2", "user-1"])

        self.assertEqual(names, {"user-1": "甲", "user-2": "user-2"})
        self.assertEqual(len(store.calls), 1)


if __name__ == "__main__":
    unittest.main()
