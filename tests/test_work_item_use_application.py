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
            conn.execute("INSERT INTO back VALUES(%s,%s,%s,%s)", ("u", 20015, 2, 1))
            conn.execute(
                "CREATE TABLE user_xiuxian(user_id TEXT PRIMARY KEY,level INTEGER,exp INTEGER)"
            )
            conn.execute("INSERT INTO user_xiuxian VALUES('u',10,12345)")
        from nonebot_plugin_xiuxian_2.features.work.migrations import (
            apply_work_item_use,
            apply_work_offer_snapshots,
        )

        with DatabaseUnitOfWork(self.database) as uow:
            apply_work_item_use(uow)
            apply_work_offer_snapshots(uow)
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

    def capture(self, operation_id: str = "capture", offer=None, reward_multiplier=None, **overrides):
        values = {
            "user_id": "u",
            "item_id": 20015,
            "expected_item_count": 2,
            "expected_work_type": 0,
            "new_offer": offer or self.offer(),
            "reward_multiplier": reward_multiplier,
        }
        values.update(overrides)
        return self.application.capture(operation_id, **values)

    @staticmethod
    def offer(award=30):
        return {
            "tasks": {
                "采药": {
                    "award": award,
                    "rate": 100,
                    "time": 10,
                    "item_id": 1,
                    "success_msg": "success",
                    "fail_msg": "failure",
                }
            },
            "task_order": ["采药"],
            "status": 1,
            "refresh_time": "2026-07-13 11:00:00.000000",
            "user_level": 10,
        }

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

    def test_capture_consumes_item_and_persists_offer_atomically(self) -> None:
        with db_backend.transaction(self.database) as conn:
            conn.execute("UPDATE user_cd SET type=0,create_time='0',scheduled_time=NULL WHERE user_id='u'")

        result = self.capture()

        self.assertEqual((result.status, result.item_remaining), ("applied", 1))
        self.assertEqual(result.result_snapshot, {"offer": self.offer()})
        with db_backend.connection(self.database) as conn:
            item = conn.execute(
                "SELECT goods_num,bind_num FROM back WHERE user_id='u' AND goods_id=20015"
            ).fetchone()
            snapshot = conn.execute(
                "SELECT snapshot,updated_at FROM work_offer_snapshots WHERE user_id='u'"
            ).fetchone()
        self.assertEqual(tuple(item), (1, 0))
        self.assertEqual(json.loads(snapshot[0]), self.offer())
        self.assertEqual(snapshot[1], "2026-07-13 11:00:00.000000")

    def test_offer_snapshot_migration_preserves_existing_rows(self) -> None:
        from nonebot_plugin_xiuxian_2.features.work.migrations import apply_work_offer_snapshots

        with db_backend.transaction(self.database) as conn:
            conn.execute(
                "INSERT INTO work_offer_snapshots VALUES(%s,%s,%s)",
                ("legacy-user", json.dumps(self.offer()), "2026-07-13 11:00:00.000000"),
            )
        with DatabaseUnitOfWork(self.database) as uow:
            apply_work_offer_snapshots(uow)
        with db_backend.connection(self.database) as conn:
            row = conn.execute(
                "SELECT snapshot,updated_at FROM work_offer_snapshots WHERE user_id='legacy-user'"
            ).fetchone()
        self.assertEqual((json.loads(row[0]), row[1]), (self.offer(), "2026-07-13 11:00:00.000000"))

    def test_capture_replay_keeps_first_random_offer_and_rejects_changed_inputs(self) -> None:
        with db_backend.transaction(self.database) as conn:
            conn.execute("UPDATE user_cd SET type=0,create_time='0',scheduled_time=NULL WHERE user_id='u'")

        first = self.capture("capture-repeat", offer=self.offer(30))
        duplicate = self.capture("capture-repeat", offer=self.offer(150))
        conflict = self.capture("capture-repeat", offer=self.offer(150), expected_item_count=1)

        self.assertEqual((first.status, duplicate.status, conflict.status), ("applied", "duplicate", "operation_conflict"))
        self.assertEqual(duplicate.result_snapshot, {"offer": self.offer(30)})
        with db_backend.connection(self.database) as conn:
            self.assertEqual(
                conn.execute("SELECT goods_num FROM back WHERE goods_id=20015").fetchone()[0], 1
            )
            self.assertEqual(
                json.loads(conn.execute("SELECT snapshot FROM work_offer_snapshots WHERE user_id='u'").fetchone()[0]),
                self.offer(30),
            )

    def test_capture_replay_keeps_first_reward_multiplier(self) -> None:
        with db_backend.transaction(self.database) as conn:
            conn.execute("UPDATE user_cd SET type=0,create_time='0',scheduled_time=NULL WHERE user_id='u'")

        first = self.capture("capture-multiplier", offer=self.offer(90), reward_multiplier=3)
        duplicate = self.capture("capture-multiplier", offer=self.offer(150), reward_multiplier=5)

        self.assertEqual(first.result_snapshot["reward_multiplier"], 3)
        self.assertEqual(duplicate.status, "duplicate")
        self.assertEqual(duplicate.result_snapshot, {"offer": self.offer(90), "reward_multiplier": 3})

    def test_new_application_replays_legacy_capture_with_original_offer(self) -> None:
        from nonebot_plugin_xiuxian_2.xiuxian.xiuxian_work.transaction_service import (
            WorkItemUseService,
        )

        with db_backend.transaction(self.database) as conn:
            conn.execute("UPDATE user_cd SET type=0,create_time='0',scheduled_time=NULL WHERE user_id='u'")
        legacy_offer = self.offer(30)
        legacy = WorkItemUseService(self.database).capture(
            "legacy-capture", "u", 20015, 2, 0, legacy_offer
        )
        replay = self.capture("legacy-capture", offer=self.offer(150))

        self.assertEqual((legacy.status, replay.status), ("applied", "duplicate"))
        self.assertEqual(replay.result_snapshot, {"offer": legacy_offer})
        with db_backend.connection(self.database) as conn:
            self.assertEqual(
                conn.execute("SELECT goods_num FROM back WHERE goods_id=20015").fetchone()[0], 1
            )

    def test_capture_refuses_active_work_and_rolls_back_operation_failure(self) -> None:
        wrong_state = self.capture("active-work")
        with db_backend.transaction(self.database) as conn:
            conn.execute("UPDATE user_cd SET type=0,create_time='0',scheduled_time=NULL WHERE user_id='u'")
            conn.execute(
                "CREATE TRIGGER fail_capture_operation BEFORE INSERT ON work_item_use_operations "
                "BEGIN SELECT RAISE(ABORT,'failed'); END"
            )

        with self.assertRaises(db_backend.IntegrityError):
            self.capture("capture-rollback")
        with db_backend.connection(self.database) as conn:
            item_count = conn.execute("SELECT goods_num FROM back WHERE goods_id=20015").fetchone()[0]
            snapshot = conn.execute("SELECT COUNT(*) FROM work_offer_snapshots").fetchone()[0]
            operations = conn.execute("SELECT COUNT(*) FROM work_item_use_operations").fetchone()[0]
        self.assertEqual(wrong_state.status, "state_changed")
        self.assertEqual((item_count, snapshot, operations), (2, 0, 0))

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
                conn.execute("INSERT INTO user_cd VALUES('u',0,'0',NULL)")
                conn.execute("INSERT INTO back VALUES('u',20014,1,0)")
                conn.execute("INSERT INTO back VALUES('u',20015,1,0)")
            from nonebot_plugin_xiuxian_2.features.work.migrations import apply_work_item_use

            with DatabaseUnitOfWork(database) as uow:
                apply_work_item_use(uow)
            with self.assertRaises(sqlite3.OperationalError):
                self.application.__class__(database).capture(
                    "missing-schema", "u", 20015, 1, 0, self.offer()
                )
            with DatabaseUnitOfWork(database) as uow:
                self.assertIsNone(uow.query_one("SELECT name FROM sqlite_master WHERE name='work_offer_snapshots'"))

    def test_migration_is_routed_only_to_game_database(self) -> None:
        migrations = build_migrations()
        routed = {
            key: {migration.version for migration in migrations_for_database(migrations, key)}
            for key in ("game_db", "player_db", "trade_db", "impart_db", "message_db")
        }
        self.assertIn("work.003", routed["game_db"])
        self.assertIn("work.004", routed["game_db"])
        for key in routed:
            if key != "game_db":
                self.assertNotIn("work.003", routed[key])
                self.assertNotIn("work.004", routed[key])

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

    def test_registered_item_matcher_routes_20015_to_feature_application(self) -> None:
        back_module = importlib.import_module("nonebot_plugin_xiuxian_2.xiuxian.xiuxian_back")
        work_module = importlib.import_module("nonebot_plugin_xiuxian_2.xiuxian.xiuxian_work")
        registered_handler = next(
            handler for handler in back_module.use_item.handlers if handler.call is back_module.use_item_
        )
        fake_bot = SimpleNamespace(self_id="bot")
        fake_event = SimpleNamespace(message_id="work-capture-message")
        generated_offer = self.offer()
        with db_backend.transaction(self.database) as conn:
            conn.execute("UPDATE user_cd SET type=0,create_time='0',scheduled_time=NULL WHERE user_id='u'")

        class MessageData:
            @staticmethod
            def goods_num(user_id, item_id):
                with db_backend.connection(self.database) as conn:
                    row = conn.execute(
                        "SELECT goods_num FROM back WHERE user_id=%s AND goods_id=%s",
                        (user_id, item_id),
                    ).fetchone()
                return int(row[0]) if row else 0

            @staticmethod
            def get_user_cd(user_id):
                return {"type": 0}

            @staticmethod
            def get_user_info_with_id(user_id):
                return {"level": 10, "exp": 12345}

            @staticmethod
            def get_work_num(user_id):
                return 5

        message_data = MessageData()
        send_feedback = AsyncMock()
        with (
            patch.object(back_module, "assign_bot", new=AsyncMock(return_value=(fake_bot, None))),
            patch.object(work_module, "assign_bot", new=AsyncMock(return_value=(fake_bot, None))),
            patch.object(back_module, "check_user", return_value=(True, {"user_id": "u"}, "")),
            patch.object(work_module, "check_user", return_value=(True, {"user_id": "u", "level": 10, "exp": 12345}, "")),
            patch.object(back_module, "items") as items,
            patch.object(back_module, "_sql_message", return_value=message_data),
            patch.object(work_module, "_sql_message", return_value=message_data),
            patch.object(work_module, "workhandle") as workhandle,
            patch.object(work_module, "runtime_random", SimpleNamespace(randint=lambda low, high: 3)),
            patch.object(work_module, "work_item_use_application", self.application),
            patch.object(work_module, "savef") as savef,
            patch.object(back_module, "handle_send", new=AsyncMock()),
            patch.object(work_module, "handle_send", new=send_feedback),
            patch.object(work_module, "send_work_message", new=AsyncMock()),
            patch.object(work_module, "generate_work_message", return_value="work offer message"),
            patch.object(self.application, "capture", wraps=self.application.capture) as capture,
        ):
            items.get_data_by_item_name.return_value = (20015, {"type": "特殊道具", "name": "追捕令"})
            workhandle.return_value.do_work.return_value = ("work message", generated_offer)
            with self.assertRaises(FinishedException):
                asyncio.run(registered_handler.call(fake_bot, fake_event, args="追捕令"))

        self.assertEqual(capture.call_count, 1)
        self.assertEqual(capture.call_args.args[-1], 3)
        self.assertTrue(savef.called)
        self.assertEqual(savef.call_args.args[1], self.offer(90))
        self.assertFalse(savef.call_args.kwargs.get("sync_snapshot", True))
        self.assertIn("提升3倍", send_feedback.await_args.args[2])
        with db_backend.connection(self.database) as conn:
            item_count = conn.execute(
                "SELECT goods_num FROM back WHERE user_id='u' AND goods_id=20015"
            ).fetchone()[0]
            snapshot = json.loads(
                conn.execute("SELECT snapshot FROM work_offer_snapshots WHERE user_id='u'").fetchone()[0]
            )
        self.assertEqual((item_count, snapshot), (1, self.offer(90)))


if __name__ == "__main__":
    unittest.main()
