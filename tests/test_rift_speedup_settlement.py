import asyncio
import json
import tempfile
import unittest
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import AsyncMock, patch

import nonebot
from nonebot.exception import FinishedException

nonebot.init()

from nonebot_plugin_xiuxian_2.features.rift.application import RiftApplication
from nonebot_plugin_xiuxian_2.features.rift.migrations import apply_rift_speedup_operations
from nonebot_plugin_xiuxian_2.infrastructure.database import DatabaseUnitOfWork
from nonebot_plugin_xiuxian_2.plugin import apply_platform_schema
from nonebot_plugin_xiuxian_2.xiuxian.xiuxian_rift.transaction_service import RiftSpeedupService
from tests.test_db_backend import db_backend


class RiftSpeedupSettlementTests(unittest.TestCase):
    def test_rift_speedup_handler_uses_feature_application(self):
        source = Path("nonebot_plugin_xiuxian_2/xiuxian/xiuxian_rift/__init__.py").read_text(encoding="utf-8")
        handler = source[source.index("async def _use_rift_speedup"):]
        self.assertIn("rift_application.speedup(", handler)

    def test_handler_keeps_authoritative_rejection_when_payload_has_status(self):
        rift_module = __import__(
            "nonebot_plugin_xiuxian_2.xiuxian.xiuxian_rift",
            fromlist=["_use_rift_speedup"],
        )
        bot = SimpleNamespace(self_id="bot")
        event = SimpleNamespace(message_id="speedup-rejection")
        outcome = SimpleNamespace(
            status="item_missing",
            ok=False,
            data={"status": "stale-payload-status", "reason": "item_missing"},
        )
        send = AsyncMock()

        async def run():
            with (
                patch.object(rift_module, "assign_bot", new=AsyncMock(return_value=(bot, None))),
                patch.object(rift_module, "check_user", return_value=(True, {"user_id": "u"}, "")),
                patch.object(rift_module, "rift_application", SimpleNamespace(speedup=lambda **_: outcome)),
                patch.object(rift_module, "handle_send", new=send),
            ):
                await rift_module._use_rift_speedup(bot, event, 20012, 50, "50%")

        asyncio.run(run())
        send.assert_awaited_once()
        self.assertEqual(send.await_args.args[2], "加速券数量不足，请重新查看背包。")

    def test_item_handler_parameter_chain_preserves_normal_and_big_speedup_ratios(self):
        rift_module = __import__(
            "nonebot_plugin_xiuxian_2.xiuxian.xiuxian_rift",
            fromlist=["use_rift_speedup", "use_rift_big_speedup"],
        )
        bot = SimpleNamespace(self_id="bot")
        calls = []

        def speedup(**kwargs):
            calls.append(kwargs)
            return SimpleNamespace(
                status="item_missing",
                ok=False,
                data={"status": "stale-payload-status"},
            )

        async def run():
            with (
                patch.object(rift_module, "assign_bot", new=AsyncMock(return_value=(bot, None))),
                patch.object(rift_module, "check_user", return_value=(True, {"user_id": "u"}, "")),
                patch.object(rift_module, "rift_application", SimpleNamespace(speedup=speedup)),
                patch.object(rift_module, "handle_send", new=AsyncMock()),
            ):
                await rift_module.use_rift_speedup(bot, SimpleNamespace(message_id="normal"), 20012, 1)
                await rift_module.use_rift_big_speedup(bot, SimpleNamespace(message_id="big"), 20013, 1)

        asyncio.run(run())
        self.assertEqual(
            [(call["item_id"], call["remaining_ratio"]) for call in calls],
            [(20012, 50), (20013, 10)],
        )

    def test_real_application_handler_persists_normal_and_big_speedup_operations(self):
        rift_module = __import__(
            "nonebot_plugin_xiuxian_2.xiuxian.xiuxian_rift",
            fromlist=["use_rift_speedup", "use_rift_big_speedup"],
        )
        with DatabaseUnitOfWork(self.database) as uow:
            apply_platform_schema(uow)
            apply_rift_speedup_operations(uow)

        application = RiftApplication(self.database, Path(self.temp_dir.name) / "player.db")
        bot = SimpleNamespace(self_id="bot")
        replies = []

        async def capture_reply(*args, **kwargs):
            replies.append((args, kwargs))

        async def run():
            with (
                patch.object(rift_module, "assign_bot", new=AsyncMock(return_value=(bot, None))),
                patch.object(
                    rift_module,
                    "check_user",
                    return_value=(True, {"user_id": "u"}, ""),
                ),
                patch.object(rift_module, "rift_application", application),
                patch.object(rift_module, "save_rift_data"),
                patch.object(rift_module, "handle_send", new=capture_reply),
            ):
                await rift_module.use_rift_speedup(
                    bot, SimpleNamespace(message_id="normal-real"), 20012, 1
                )
                await rift_module.use_rift_big_speedup(
                    bot, SimpleNamespace(message_id="big-real"), 20013, 1
                )

        asyncio.run(run())
        self.assertEqual(len(replies), 2)
        self.assertIn("减少50%", replies[0][0][2])
        self.assertIn("减少90%", replies[1][0][2])
        with db_backend.connection(self.database) as conn:
            operations = conn.execute(
                "SELECT operation_id,payload,new_time FROM rift_speedup_operations "
                "ORDER BY operation_id"
            ).fetchall()
            self.assertEqual(
                [(row[0], json.loads(row[1])[-1], row[2]) for row in operations],
                [
                    ("rift-speedup:big-real:u", 10, 3),
                    ("rift-speedup:normal-real:u", 50, 30),
                ],
            )
            self.assertEqual(
                [tuple(row) for row in
                    conn.execute(
                        "SELECT goods_id,goods_num FROM back "
                        "WHERE user_id='u' ORDER BY goods_id"
                    ).fetchall()],
                [(20012, 0), (20013, 0)],
            )

    def test_registered_item_command_retries_after_receipt_rejection(self):
        back_module = __import__(
            "nonebot_plugin_xiuxian_2.xiuxian.xiuxian_back", fromlist=["use_item"]
        )
        rift_module = __import__(
            "nonebot_plugin_xiuxian_2.xiuxian.xiuxian_rift", fromlist=["use_rift_speedup"]
        )
        registered = next(
            handler for handler in back_module.use_item.handlers if handler.call is back_module.use_item_
        )
        with DatabaseUnitOfWork(self.database) as uow:
            apply_platform_schema(uow)
            apply_rift_speedup_operations(uow)
        with db_backend.transaction(self.database) as conn:
            conn.execute(
                "CREATE TRIGGER reject_receipt BEFORE INSERT ON rift_speedup_operations "
                "BEGIN SELECT RAISE(IGNORE); END"
            )

        application = RiftApplication(self.database, Path(self.temp_dir.name) / "player.db")
        bot = SimpleNamespace(self_id="bot")
        event = SimpleNamespace(message_id="same-speedup-message")
        replies = []

        class Items:
            @staticmethod
            def get_data_by_item_name(name):
                return (20012, {"type": "特殊道具", "name": "秘境加速券"})

        class MessageData:
            @staticmethod
            def goods_num(user_id, item_id):
                return 1

        async def capture_reply(*args, **kwargs):
            replies.append((args, kwargs))

        async def invoke():
            with (
                patch.object(back_module, "assign_bot", new=AsyncMock(return_value=(bot, None))),
                patch.object(back_module, "check_user", return_value=(True, {"user_id": "u"}, "")),
                patch.object(back_module, "items", Items()),
                patch.object(back_module, "_sql_message", return_value=MessageData()),
                patch.object(back_module, "handle_send", new=capture_reply),
                patch.object(rift_module, "assign_bot", new=AsyncMock(return_value=(bot, None))),
                patch.object(rift_module, "check_user", return_value=(True, {"user_id": "u"}, "")),
                patch.object(rift_module, "rift_application", application),
                patch.object(rift_module, "save_rift_data"),
                patch.object(rift_module, "handle_send", new=capture_reply),
            ):
                with self.assertRaises(FinishedException):
                    await registered.call(bot, event, args="秘境加速券")
                with db_backend.transaction(self.database) as conn:
                    conn.execute("DROP TRIGGER reject_receipt")
                with self.assertRaises(FinishedException):
                    await registered.call(bot, event, args="秘境加速券")

        asyncio.run(invoke())
        self.assertEqual(len(replies), 2)
        self.assertIn("回执保存失败", replies[0][0][2])
        self.assertIn("减少50%", replies[1][0][2])
        with db_backend.connection(self.database) as conn:
            self.assertEqual(conn.execute(
                "SELECT goods_num FROM back WHERE user_id='u' AND goods_id=20012"
            ).fetchone()[0], 0)
            self.assertEqual(conn.execute(
                "SELECT duration FROM rift_entries WHERE user_id='u'"
            ).fetchone()[0], 30)
            self.assertEqual(conn.execute(
                "SELECT COUNT(*) FROM rift_speedup_operations"
            ).fetchone()[0], 1)

    def setUp(self):
        self.temp_dir = tempfile.TemporaryDirectory()
        self.database = Path(self.temp_dir.name) / "game.db"
        self.rift = {"name": "test", "rank": 2, "time": 60}
        self.cd = {"type": 3, "create_time": "2026-07-14 10:00:00.000000", "scheduled_time": "60"}
        with db_backend.transaction(self.database) as conn:
            conn.execute(
                "CREATE TABLE rift_entries(user_id TEXT PRIMARY KEY,rift_data TEXT,status TEXT,duration INTEGER)"
            )
            conn.execute(
                "CREATE TABLE user_cd(user_id TEXT PRIMARY KEY,type INTEGER,create_time TEXT,scheduled_time TEXT)"
            )
            conn.execute(
                "CREATE TABLE back(user_id TEXT,goods_id INTEGER,goods_num INTEGER,"
                "bind_num INTEGER DEFAULT 0)"
            )
            conn.execute("INSERT INTO rift_entries VALUES('u',%s,'active',60)", (json.dumps(self.rift),))
            conn.execute("INSERT INTO user_cd VALUES('u',3,%s,'60')", (self.cd["create_time"],))
            conn.execute("INSERT INTO back VALUES('u',20012,1,1)")
            conn.execute("INSERT INTO back VALUES('u',20013,1,1)")
        self.service = RiftSpeedupService(self.database)

    def tearDown(self):
        self.temp_dir.cleanup()

    def test_normal_speedup_is_atomic_and_idempotent(self):
        first = self.service.apply("op", "u", 20012, self.rift, self.cd, 50)
        duplicate = self.service.apply("op", "u", 20012, self.rift, self.cd, 50)
        self.assertEqual((first.status, first.new_time, duplicate.status), ("applied", 30, "duplicate"))
        self.assertEqual(first.rift_data["time"], 30)
        with db_backend.connection(self.database) as conn:
            entry = conn.execute("SELECT rift_data,duration FROM rift_entries").fetchone()
            self.assertEqual((json.loads(entry[0])["time"], entry[1]), (30, 30))
            self.assertEqual(conn.execute("SELECT scheduled_time FROM user_cd").fetchone()[0], "30")
            self.assertEqual(
                tuple(
                    conn.execute(
                        "SELECT goods_num,bind_num FROM back WHERE goods_id=20012"
                    ).fetchone()
                ),
                (0, 0),
            )

    def test_production_entry_rechecks_database_and_retries_idempotently(self):
        first = self.service.apply("event", "u", 20012, remaining_ratio=50)
        duplicate = self.service.apply("event", "u", 20012, remaining_ratio=50)
        self.assertEqual((first.status, duplicate.status, duplicate.new_time), ("applied", "duplicate", 30))
        self.assertEqual(duplicate.rift_data, {**self.rift, "time": 30})
        self.assertEqual(duplicate.create_time, self.cd["create_time"])
        with db_backend.connection(self.database) as conn:
            self.assertEqual(conn.execute("SELECT goods_num FROM back WHERE goods_id=20012").fetchone()[0], 0)

    def test_mixed_inventory_consumes_bound_item_and_preserves_invariant(self):
        with db_backend.transaction(self.database) as conn:
            conn.execute(
                "UPDATE back SET goods_num=3,bind_num=1 WHERE goods_id=20012"
            )
        result = self.service.apply(
            "mixed-bound", "u", 20012, self.rift, self.cd, 50
        )
        self.assertEqual("applied", result.status)
        with db_backend.connection(self.database) as conn:
            self.assertEqual(
                (2, 0),
                tuple(
                    conn.execute(
                        "SELECT goods_num,bind_num FROM back WHERE goods_id=20012"
                    ).fetchone()
                ),
            )

    def test_production_entry_rejects_inconsistent_rift_state_without_consuming_item(self):
        with db_backend.transaction(self.database) as conn:
            stale = {**self.rift, "time": 59}
            conn.execute("UPDATE rift_entries SET rift_data=%s,duration=60", (json.dumps(stale),))
        result = self.service.apply("inconsistent", "u", 20012, remaining_ratio=50)
        self.assertEqual(result.status, "state_changed")
        with db_backend.connection(self.database) as conn:
            self.assertEqual(conn.execute("SELECT goods_num FROM back WHERE goods_id=20012").fetchone()[0], 1)
            self.assertFalse(conn.table_exists("rift_speedup_operations"))

    def test_big_speedup_uses_same_transaction_kernel(self):
        result = self.service.apply("big", "u", 20013, self.rift, self.cd, 10)
        self.assertEqual((result.status, result.new_time), ("applied", 6))
        with db_backend.connection(self.database) as conn:
            self.assertEqual(
                tuple(
                    conn.execute(
                        "SELECT goods_num,bind_num FROM back WHERE goods_id=20013"
                    ).fetchone()
                ),
                (0, 0),
            )
            self.assertEqual(conn.execute("SELECT duration FROM rift_entries").fetchone()[0], 6)

    def test_minimum_one_minute_and_no_speedup_boundary(self):
        with db_backend.transaction(self.database) as conn:
            small = {**self.rift, "time": 11}
            conn.execute("UPDATE rift_entries SET rift_data=%s,duration=11", (json.dumps(small),))
            conn.execute("UPDATE user_cd SET scheduled_time='11'")
        cd = {**self.cd, "scheduled_time": "11"}
        result = self.service.apply("minimum", "u", 20013, small, cd, 10)
        self.assertEqual(result.new_time, 1)

        with db_backend.transaction(self.database) as conn:
            short = {**self.rift, "time": 10}
            conn.execute("UPDATE rift_entries SET rift_data=%s,duration=10", (json.dumps(short),))
            conn.execute("UPDATE user_cd SET scheduled_time='10'")
            conn.execute("UPDATE back SET goods_num=1 WHERE goods_id=20012")
        result = self.service.apply("not-needed", "u", 20012, short, {**self.cd, "scheduled_time": "10"}, 50)
        self.assertEqual(result.status, "not_needed")
        with db_backend.connection(self.database) as conn:
            self.assertEqual(conn.execute("SELECT goods_num FROM back WHERE goods_id=20012").fetchone()[0], 1)

    def test_state_item_and_payload_conflicts_are_rejected(self):
        self.assertEqual(
            self.service.apply("state", "u", 20012, self.rift, {**self.cd, "scheduled_time": "59"}, 50).status,
            "state_changed",
        )
        with db_backend.transaction(self.database) as conn:
            conn.execute("UPDATE back SET goods_num=0 WHERE goods_id=20012")
        self.assertEqual(self.service.apply("missing", "u", 20012, self.rift, self.cd, 50).status, "item_missing")
        with db_backend.transaction(self.database) as conn:
            conn.execute("UPDATE back SET goods_num=1 WHERE goods_id=20012")
        self.assertEqual(self.service.apply("op", "u", 20012, self.rift, self.cd, 50).status, "applied")
        self.assertEqual(self.service.apply("op", "u", 20012, self.rift, self.cd, 10).status, "state_changed")

    def test_operation_failure_rolls_back_all_state(self):
        with db_backend.transaction(self.database) as conn:
            conn.execute(
                "CREATE TABLE rift_speedup_operations("
                "operation_id TEXT PRIMARY KEY,payload TEXT,new_time INTEGER,rift_data TEXT,created_at TEXT)"
            )
            conn.execute(
                "CREATE TRIGGER fail_speedup BEFORE INSERT ON rift_speedup_operations "
                "BEGIN SELECT RAISE(ABORT,'fail'); END"
            )
        with self.assertRaises(Exception):
            self.service.apply("fail", "u", 20012, self.rift, self.cd, 50)
        with db_backend.connection(self.database) as conn:
            entry = conn.execute("SELECT rift_data,duration FROM rift_entries").fetchone()
            self.assertEqual((json.loads(entry[0])["time"], entry[1]), (60, 60))
            self.assertEqual(conn.execute("SELECT scheduled_time FROM user_cd").fetchone()[0], "60")
            self.assertEqual(
                (1, 1),
                tuple(
                    conn.execute(
                        "SELECT goods_num,bind_num FROM back WHERE goods_id=20012"
                    ).fetchone()
                ),
            )

    def test_existing_operation_table_is_migrated(self):
        with db_backend.transaction(self.database) as conn:
            conn.execute(
                "CREATE TABLE rift_speedup_operations("
                "operation_id TEXT PRIMARY KEY,payload TEXT,new_time INTEGER)"
            )
        result = self.service.apply("migrated", "u", 20012, self.rift, self.cd, 50)
        self.assertEqual(result.status, "applied")
        with db_backend.connection(self.database) as conn:
            self.assertTrue(conn.column_exists("rift_speedup_operations", "rift_data"))
            self.assertTrue(conn.column_exists("rift_speedup_operations", "create_time"))


if __name__ == "__main__":
    unittest.main()
