from __future__ import annotations

import sqlite3
import tempfile
from pathlib import Path
import unittest
from unittest.mock import patch

import nonebot

nonebot.init()

from nonebot.exception import FinishedException, PausedException, RejectedException
from nonebot_plugin_xiuxian_2.core.errors import DomainError, OperationConflictError
from nonebot_plugin_xiuxian_2.xiuxian.command_guard import failure_reply, guard_command
from nonebot_plugin_xiuxian_2.xiuxian.xiuxian_pet.failure_reply import guard_pet_handler
from tests import test_partner_cultivation_application as _partner
from nonebot_plugin_xiuxian_2.features.impart.repository import ImpartRepository
from nonebot_plugin_xiuxian_2.infrastructure.database import DatabaseUnitOfWork
from nonebot_plugin_xiuxian_2.plugin import build_migrations, migrations_for_database


class ImpartInitializationTests(unittest.TestCase):
    def test_startup_migrations_and_first_user_are_idempotent(self):
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "impart.db"
            with DatabaseUnitOfWork(path) as uow:
                for migration in migrations_for_database(build_migrations(), "impart_db"):
                    migration.apply(uow)
            repository = ImpartRepository(path)
            self.assertIsNotNone(repository.state("new", ensure=True))
            self.assertIsNotNone(repository.state("new", ensure=True))
            with sqlite3.connect(path) as conn:
                self.assertEqual(conn.execute("SELECT count(*) FROM xiuxian_impart WHERE user_id='new'").fetchone()[0], 1)


class PartnerReasonTests(unittest.TestCase):
    setUp = _partner.PartnerCultivationApplicationTests.setUp
    tearDown = _partner.PartnerCultivationApplicationTests.tearDown
    apply = _partner.PartnerCultivationApplicationTests.apply
    state = _partner.PartnerCultivationApplicationTests.state
    def test_usage_and_exp_rejections_preserve_subject(self):
        usage = self.apply(expected_used_count_2=1)
        exp = self.apply(expected_exp_1=999)
        self.assertEqual((usage.status, usage.reason, usage.subject),
                         ("state_changed", "usage_changed", "b"))
        self.assertEqual((exp.status, exp.reason, exp.subject),
                         ("state_changed", "exp_changed", "a"))
        self.assertEqual([1000, 2000], [int(row[1]) for row in self.state()[0]])

    def test_invite_missing_and_expired_are_distinct(self):
        missing = self.apply(invite_id="missing")
        with _partner.db_backend.transaction(self.player) as conn:
            conn.execute("INSERT INTO partner_cultivation_invites VALUES"
                         "('expired','a','b',2,'pending',900,999,NULL)")
        expired = self.apply(invite_id="expired")
        self.assertEqual(missing.reason, "invite_missing")
        self.assertEqual(expired.reason, "invite_expired")


class FailureReplyTests(unittest.IsolatedAsyncioTestCase):
    async def test_known_codes_are_distinct_without_sending_exception_text(self):
        missing = failure_reply("喂食", DomainError("pet_missing", "SELECT /secret.db"))
        item = failure_reply("喂食", DomainError("item_missing", "SELECT /secret.db"))
        self.assertNotEqual(missing, item)
        self.assertNotIn("SELECT", missing + item)
        conflict = failure_reply("双修", OperationConflictError("private", "x"))
        self.assertIn("操作号", conflict)
        self.assertNotIn("处理中", conflict)

    async def test_framework_control_flow_propagates_for_both_guards(self):
        for guard in (guard_command, guard_pet_handler):
            for control in (FinishedException, PausedException, RejectedException):
                calls = []
                async def send(*args):
                    calls.append(args)
                @guard("双修", send=send)
                async def handler(bot, event):
                    raise control
                with self.assertRaises(control):
                    await handler(None, None)
                self.assertEqual(calls, [])

    async def test_post_commit_failure_makes_no_rollback_promise(self):
        calls = []
        async def send(bot, event, text):
            calls.append(text)
        with sqlite3.connect(":memory:") as conn:
            conn.execute("CREATE TABLE receipt(id TEXT)")
            @guard_command("双修", send=send)
            async def handler(bot, event):
                conn.execute("INSERT INTO receipt VALUES('done')")
                conn.commit()
                raise RuntimeError("send /private/game.db failed")
            await handler(None, None)
            self.assertEqual(conn.execute("SELECT count(*) FROM receipt").fetchone()[0], 1)
        self.assertEqual(len(calls), 1)
        for text in ("未结算", "未扣", "/private", "game.db"):
            self.assertNotIn(text, calls[0])
        self.assertIn("定位号", calls[0])


if __name__ == "__main__":
    unittest.main()
