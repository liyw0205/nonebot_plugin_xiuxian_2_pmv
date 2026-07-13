import json
import tempfile
import unittest
from pathlib import Path

import nonebot

nonebot.init()

from nonebot_plugin_xiuxian_2.xiuxian.xiuxian_title.title_transaction_service import TitleTransactionService
from tests.test_db_backend import db_backend


class TitleBatchUnlockTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.db = Path(self.temp.name) / "player.db"
        with db_backend.transaction(self.db) as conn:
            conn.execute("CREATE TABLE title(user_id TEXT PRIMARY KEY,unlocked TEXT,equipped TEXT)")
            conn.execute("INSERT INTO title VALUES('u',%s,'1')", (json.dumps(["1"]),))
        self.service = TitleTransactionService(self.db)

    def tearDown(self):
        self.temp.cleanup()

    def test_batch_unlock_and_replay(self):
        self.assertEqual(self.service.unlock_batch("op", "u", ["1"], ["2", "3"]).status, "applied")
        self.assertEqual(self.service.unlock_batch("op", "u", ["1"], ["2", "3"]).status, "duplicate")
        with db_backend.connection(self.db) as conn:
            self.assertEqual(set(json.loads(conn.execute("SELECT unlocked FROM title").fetchone()[0])), {"1", "2", "3"})

    def test_state_and_payload_conflicts(self):
        self.assertEqual(self.service.unlock_batch("stale", "u", [], ["2"]).status, "state_changed")
        self.service.unlock_batch("same", "u", ["1"], ["2"])
        self.assertEqual(self.service.unlock_batch("same", "u", ["1"], ["3"]).status, "operation_conflict")

    def test_failure_rolls_back(self):
        with db_backend.transaction(self.db) as conn:
            conn.execute("CREATE TABLE title_transaction_operations(operation_id TEXT PRIMARY KEY,payload TEXT,result_status TEXT,title_id TEXT,created_at TEXT)")
            conn.execute("CREATE TRIGGER fail_title_unlock BEFORE INSERT ON title_transaction_operations BEGIN SELECT RAISE(ABORT,'failed'); END")
        with self.assertRaises(Exception):
            self.service.unlock_batch("fail", "u", ["1"], ["2"])
        with db_backend.connection(self.db) as conn:
            self.assertEqual(json.loads(conn.execute("SELECT unlocked FROM title").fetchone()[0]), ["1"])
