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
