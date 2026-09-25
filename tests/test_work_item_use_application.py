from __future__ import annotations

import asyncio
import importlib
import json
import sqlite3
import tempfile
import unittest
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import AsyncMock, patch

import nonebot
from nonebot.exception import FinishedException

from nonebot_plugin_xiuxian_2.infrastructure.database import DatabaseUnitOfWork
from nonebot_plugin_xiuxian_2.plugin import build_migrations, migrations_for_database
from tests.test_db_backend import db_backend


class WorkItemUseApplicationTests(unittest.TestCase):
    def setUp(self) -> None:
        self.temp = tempfile.TemporaryDirectory()
        self.database = Path(self.temp.name) / "game.sqlite3"
        with db_backend.transaction(self.database) as conn:
            conn.execute(
                "CREATE TABLE user_cd(user_id TEXT PRIMARY KEY,type INTEGER,create_time TEXT,scheduled_time TEXT)"
            )
            conn.execute(
                "INSERT INTO user_cd VALUES(%s,%s,%s,%s)",
                ("u", 2, "2026-07-13 10:00:00", "镇妖"),
            )
            conn.execute(
                "CREATE TABLE back(user_id TEXT,goods_id INTEGER,goods_num INTEGER,bind_num INTEGER,"
                "UNIQUE(user_id,goods_id))"
            )
            conn.execute("INSERT INTO back VALUES(%s,%s,%s,%s)", ("u", 20014, 2, 1))
            conn.execute(
                "CREATE TABLE user_xiuxian(user_id TEXT PRIMARY KEY,level INTEGER,exp INTEGER)"
            )
            conn.execute("INSERT INTO user_xiuxian VALUES('u',10,12345)")
        from nonebot_plugin_xiuxian_2.features.work.migrations import apply_work_item_use

        with DatabaseUnitOfWork(self.database) as uow:
            apply_work_item_use(uow)
        from nonebot_plugin_xiuxian_2.features.work.work_item_use_application import (
            WorkItemUseApplication,
        )

        self.application = WorkItemUseApplication(self.database)
        self.expected_work = {
            "type": 2,
            "create_time": "2026-07-13 10:00:00",
            "scheduled_time": "镇妖",
        }

    def tearDown(self) -> None:
        self.temp.cleanup()

    def accelerate(self, operation_id: str = "speed", **overrides):
        values = {
            "user_id": "u",
            "item_id": 20014,
            "expected_item_count": 2,
            "expected_work": self.expected_work,
            "accelerated_at": "1970-01-01 00:00:00",
        }
        values.update(overrides)
        return self.application.accelerate(operation_id, **values)

    def test_accelerate_consumes_one_item_and_preserves_work_result_snapshot(self) -> None:
        result = self.accelerate()

        self.assertEqual((result.status, result.item_remaining), ("applied", 1))
        with db_backend.connection(self.database) as conn:
            work = conn.execute(
                "SELECT type,create_time,scheduled_time FROM user_cd WHERE user_id=%s", ("u",)
            ).fetchone()
            item = conn.execute(
                "SELECT goods_num,bind_num FROM back WHERE user_id=%s AND goods_id=%s",
                ("u", 20014),
            ).fetchone()
            cultivation = conn.execute(
                "SELECT level,exp FROM user_xiuxian WHERE user_id=%s", ("u",)
            ).fetchone()
            operation = conn.execute(
                "SELECT action,item_remaining,result_snapshot FROM work_item_use_operations "
                "WHERE operation_id=%s",
                ("speed",),
            ).fetchone()
        self.assertEqual(tuple(work), (2, "1970-01-01 00:00:00", "镇妖"))
        self.assertEqual(tuple(item), (1, 0))
        self.assertEqual(tuple(cultivation), (10, 12345))
        self.assertEqual((operation[0], operation[1]), ("accelerate", 1))
        self.assertEqual(json.loads(operation[2]), {"accelerated_at": "1970-01-01 00:00:00"})

    def test_replay_conflict_and_stale_work_snapshot(self) -> None:
        stale_inventory = self.accelerate("inventory-stale", expected_item_count=3)
        first = self.accelerate("same")
        duplicate = self.accelerate("same")
        conflict = self.accelerate("same", accelerated_at="1969-01-01 00:00:00")
        stale = self.accelerate("stale")

        self.assertEqual(
            (stale_inventory.status, first.status, duplicate.status, conflict.status, stale.status),
            ("state_changed", "applied", "duplicate", "operation_conflict", "state_changed"),
        )
        with db_backend.connection(self.database) as conn:
            self.assertEqual(
                conn.execute("SELECT goods_num FROM back WHERE goods_id=20014").fetchone()[0], 1
            )
            self.assertEqual(
                conn.execute("SELECT COUNT(*) FROM work_item_use_operations").fetchone()[0], 1
            )

    def test_new_application_replays_an_operation_written_by_legacy_service(self) -> None:
        from nonebot_plugin_xiuxian_2.xiuxian.xiuxian_work.transaction_service import (
            WorkItemUseService,
        )

        legacy = WorkItemUseService(self.database).accelerate(
            "legacy-operation",
            "u",
            20014,
            2,
            self.expected_work,
            "1970-01-01 00:00:00",
        )
        replay = self.accelerate("legacy-operation")

        self.assertEqual((legacy.status, replay.status), ("applied", "duplicate"))
        with db_backend.connection(self.database) as conn:
            self.assertEqual(
                conn.execute("SELECT goods_num FROM back WHERE goods_id=20014").fetchone()[0], 1
            )

    def test_missing_item_and_missing_user_are_rejected(self) -> None:
        missing_item = self.accelerate("item", item_id=999)
        with db_backend.transaction(self.database) as conn:
            conn.execute("DELETE FROM user_cd WHERE user_id='u'")
        missing_user = self.accelerate("user")

        self.assertEqual((missing_item.status, missing_user.status), ("item_missing", "user_missing"))

    def test_failure_rolls_back_item_and_work_updates(self) -> None:
        with db_backend.transaction(self.database) as conn:
            conn.execute(
                "CREATE TRIGGER fail_work_item_use BEFORE INSERT ON work_item_use_operations "
                "BEGIN SELECT RAISE(ABORT,'failed'); END"
            )

        with self.assertRaises(db_backend.IntegrityError):
            self.accelerate("rollback")
        with db_backend.connection(self.database) as conn:
            work = conn.execute("SELECT create_time FROM user_cd WHERE user_id='u'").fetchone()[0]
            item = conn.execute("SELECT goods_num,bind_num FROM back WHERE goods_id=20014").fetchone()
        self.assertEqual((work, tuple(item)), ("2026-07-13 10:00:00", (2, 1)))

    def test_inventory_compare_and_swap_failure_rolls_back_work_update(self) -> None:
        with db_backend.transaction(self.database) as conn:
            conn.execute(
                "CREATE TRIGGER ignore_item_update BEFORE UPDATE OF goods_num ON back "
                "BEGIN SELECT RAISE(IGNORE); END"
            )

        result = self.accelerate("state-changed")

        self.assertEqual(result.status, "state_changed")
        with db_backend.connection(self.database) as conn:
            work = conn.execute("SELECT create_time FROM user_cd WHERE user_id='u'").fetchone()[0]
            item_count = conn.execute("SELECT goods_num FROM back WHERE goods_id=20014").fetchone()[0]
            operation_count = conn.execute(
                "SELECT COUNT(*) FROM work_item_use_operations"
            ).fetchone()[0]
        self.assertEqual((work, item_count, operation_count), ("2026-07-13 10:00:00", 2, 0))

    def test_request_does_not_create_missing_schema(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            database = Path(directory) / "missing.sqlite3"
            with db_backend.transaction(database) as conn:
                conn.execute(
                    "CREATE TABLE user_cd(user_id TEXT PRIMARY KEY,type INTEGER,create_time TEXT,scheduled_time TEXT)"
                )
                conn.execute("CREATE TABLE back(user_id TEXT,goods_id INTEGER,goods_num INTEGER,bind_num INTEGER)")
                conn.execute("INSERT INTO user_cd VALUES('u',2,'now','镇妖')")
                conn.execute("INSERT INTO back VALUES('u',20014,1,0)")
            with self.assertRaises(sqlite3.OperationalError):
                self.application.__class__(database).accelerate(
                    "missing-schema", "u", 20014, 1, self.expected_work, "1970-01-01"
                )
            with DatabaseUnitOfWork(database) as uow:
                self.assertIsNone(
                    uow.query_one(
                        "SELECT name FROM sqlite_master WHERE name='work_item_use_operations'"
                    )
                )

    def test_migration_is_routed_only_to_game_database(self) -> None:
        migrations = build_migrations()
        routed = {
            key: {migration.version for migration in migrations_for_database(migrations, key)}
            for key in ("game_db", "player_db", "trade_db", "impart_db", "message_db")
        }
        self.assertIn("work.003", routed["game_db"])
        for key in routed:
            if key != "game_db":
                self.assertNotIn("work.003", routed[key])

    @classmethod
    def setUpClass(cls) -> None:
        try:
            nonebot.get_driver()
        except ValueError:
            nonebot.init()

    def test_registered_item_matcher_routes_20014_to_feature_application(self) -> None:
        back_module = importlib.import_module("nonebot_plugin_xiuxian_2.xiuxian.xiuxian_back")
        work_module = importlib.import_module("nonebot_plugin_xiuxian_2.xiuxian.xiuxian_work")
        registered_handler = next(
            handler for handler in back_module.use_item.handlers if handler.call is back_module.use_item_
        )
        fake_bot = SimpleNamespace(self_id="bot")
        fake_event = SimpleNamespace(message_id="work-speed-message")

        class MessageData:
            @staticmethod
            def goods_num(user_id, item_id):
                with db_backend.connection(self.database) as conn:
                    row = conn.execute(
                        "SELECT goods_num FROM back WHERE user_id=%s AND goods_id=%s",
                        (user_id, item_id),
                    ).fetchone()
                return int(row[0]) if row else 0

        message_data = MessageData()
        work_snapshot = dict(self.expected_work)
        ready_snapshot = {**work_snapshot, "create_time": "1970-01-01 00:00:00"}

        with (
            patch.object(back_module, "assign_bot", new=AsyncMock(return_value=(fake_bot, None))),
            patch.object(work_module, "assign_bot", new=AsyncMock(return_value=(fake_bot, None))),
            patch.object(back_module, "check_user", return_value=(True, {"user_id": "u"}, "")),
            patch.object(work_module, "check_user", return_value=(True, {"user_id": "u"}, "")),
            patch.object(back_module, "items") as items,
            patch.object(back_module, "_sql_message", return_value=message_data),
            patch.object(work_module, "_sql_message", return_value=message_data),
            patch.object(work_module, "get_user_work_status", side_effect=[(1, work_snapshot), (2, ready_snapshot)]),
            patch.object(work_module, "work_item_use_application", self.application),
            patch.object(
                work_module,
                "_work_item_use_service",
                side_effect=AssertionError("legacy item-use service was called"),
            ),
            patch.object(back_module, "handle_send", new=AsyncMock()),
            patch.object(work_module, "handle_send", new=AsyncMock()),
            patch.object(work_module, "settle_work", new=AsyncMock()) as settle,
            patch.object(self.application, "accelerate", wraps=self.application.accelerate) as accelerate,
        ):
            items.get_data_by_item_name.return_value = (20014, {"type": "特殊道具", "name": "悬赏令"})
            with self.assertRaises(FinishedException):
                asyncio.run(registered_handler.call(fake_bot, fake_event, args="悬赏令"))

        self.assertEqual(accelerate.call_count, 1)
        self.assertEqual(settle.await_count, 1)
        with db_backend.connection(self.database) as conn:
            item_count = conn.execute(
                "SELECT goods_num FROM back WHERE user_id='u' AND goods_id=20014"
            ).fetchone()[0]
            work_create_time = conn.execute(
                "SELECT create_time FROM user_cd WHERE user_id='u'"
            ).fetchone()[0]
        self.assertEqual((item_count, work_create_time), (1, "1970-01-01 00:00:00"))


if __name__ == "__main__":
    unittest.main()
