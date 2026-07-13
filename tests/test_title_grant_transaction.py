import json
import tempfile
import unittest
from pathlib import Path

import nonebot

nonebot.init()

from nonebot_plugin_xiuxian_2.xiuxian.xiuxian_title.title_transaction_service import TitleTransactionService
from tests.test_db_backend import db_backend


class TitleGrantTransactionTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.db = Path(self.temp.name) / "player.db"
        with db_backend.transaction(self.db) as conn:
            conn.execute("CREATE TABLE title(user_id TEXT PRIMARY KEY,unlocked TEXT,equipped TEXT)")
            conn.execute("INSERT INTO title VALUES('u',%s,'1')", (json.dumps(["1"]),))
        self.service = TitleTransactionService(self.db)

    def tearDown(self):
        self.temp.cleanup()

    def test_grant_replay_and_preserve_equipped(self):
        self.assertEqual(self.service.grant("op", "u", ["1"], "2").status, "applied")
        self.assertEqual(self.service.grant("op", "u", ["1"], "2").status, "duplicate")
        with db_backend.connection(self.db) as conn:
            row = conn.execute("SELECT unlocked,equipped FROM title").fetchone()
            self.assertEqual((set(json.loads(row[0])), row[1]), ({"1", "2"}, "1"))

    def test_rejections(self):
        self.assertEqual(self.service.grant("owned", "u", ["1"], "1").status, "already_unlocked")
        self.assertEqual(self.service.grant("stale", "u", [], "2").status, "state_changed")

    def test_failure_rolls_back(self):
        with db_backend.transaction(self.db) as conn:
            conn.execute("CREATE TABLE title_transaction_operations(operation_id TEXT PRIMARY KEY,payload TEXT,result_status TEXT,title_id TEXT,created_at TEXT)")
            conn.execute("CREATE TRIGGER fail_title_grant BEFORE INSERT ON title_transaction_operations BEGIN SELECT RAISE(ABORT,'failed'); END")
        with self.assertRaises(Exception):
            self.service.grant("fail", "u", ["1"], "2")
        with db_backend.connection(self.db) as conn:
            self.assertEqual(json.loads(conn.execute("SELECT unlocked FROM title").fetchone()[0]), ["1"])
