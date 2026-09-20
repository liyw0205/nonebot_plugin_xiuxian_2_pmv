import json
import tempfile
import unittest
from pathlib import Path

import nonebot

nonebot.init()

from nonebot_plugin_xiuxian_2.xiuxian.xiuxian_title.title_transaction_service import TitleTransactionService
from nonebot_plugin_xiuxian_2.features.title.migrations import apply_title_schema
from nonebot_plugin_xiuxian_2.infrastructure.database import DatabaseUnitOfWork
from nonebot_plugin_xiuxian_2.plugin import apply_platform_schema


def test_title_unequip_replay_uses_feature_application():
    source = Path(__file__).resolve().parents[1] / "nonebot_plugin_xiuxian_2/xiuxian/xiuxian_title/__init__.py"
    handler = source.read_text(encoding="utf-8").split("@title_unequip_cmd.handle", 1)[1].split("@title_check_cmd.handle", 1)[0]
    assert "title_application.get_result(" in handler
    assert "_title_transaction_service().get_result(" not in handler
from tests.test_db_backend import db_backend


class TitleUnequipTransactionTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.db = Path(self.temp.name) / "player.db"
        with db_backend.transaction(self.db) as conn:
            conn.execute("CREATE TABLE title(user_id TEXT PRIMARY KEY,unlocked TEXT,equipped TEXT)")
            conn.execute("INSERT INTO title VALUES(%s,%s,%s)", ("u", json.dumps(["1"]), "1"))
        with DatabaseUnitOfWork(self.db) as uow:
            apply_platform_schema(uow)
            apply_title_schema(uow)
        self.service = TitleTransactionService(self.db)

    def tearDown(self):
        self.temp.cleanup()

    def test_unequip_and_replay(self):
        self.assertEqual(self.service.unequip("op", "u", "1").status, "applied")
        self.assertEqual(self.service.unequip("op", "u", "1").status, "duplicate")
        with db_backend.connection(self.db) as conn:
            self.assertEqual(conn.execute("SELECT equipped FROM title").fetchone()[0], "")

    def test_stale_and_empty(self):
        self.assertEqual(self.service.unequip("stale", "u", "2").status, "state_changed")
        self.service.unequip("first", "u", "1")
        self.assertEqual(self.service.unequip("empty", "u", "").status, "not_equipped")

    def test_failure_rolls_back(self):
        with db_backend.transaction(self.db) as conn:
            conn.execute("CREATE TRIGGER fail_title_unequip BEFORE INSERT ON title_transaction_operations BEGIN SELECT RAISE(ABORT,'failed'); END")
        with self.assertRaises(Exception):
            self.service.unequip("fail", "u", "1")
        with db_backend.connection(self.db) as conn:
            self.assertEqual(conn.execute("SELECT equipped FROM title").fetchone()[0], "1")
