import json
import tempfile
import unittest
from pathlib import Path

import nonebot

nonebot.init()

from nonebot_plugin_xiuxian_2.xiuxian.xiuxian_title.title_transaction_service import TitleTransactionService
from tests.test_db_backend import db_backend


class TitleEquipTransactionTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.db = Path(self.temp.name) / "player.db"
        with db_backend.transaction(self.db) as conn:
            conn.execute("CREATE TABLE title(user_id TEXT PRIMARY KEY,unlocked TEXT,equipped TEXT)")
            conn.execute("INSERT INTO title VALUES(%s,%s,%s)", ("u", json.dumps(["1", "2"]), "1"))
        self.service = TitleTransactionService(self.db)

    def tearDown(self):
        self.temp.cleanup()

    def test_equip_and_replay(self):
        self.assertEqual(self.service.equip("op", "u", ["1", "2"], "1", "2").status, "applied")
        self.assertEqual(self.service.equip("op", "u", ["1", "2"], "1", "2").status, "duplicate")
        with db_backend.connection(self.db) as conn:
            self.assertEqual(conn.execute("SELECT equipped FROM title").fetchone()[0], "2")

    def test_locked_stale_and_conflict(self):
        self.assertEqual(self.service.equip("locked", "u", ["1", "2"], "1", "3").status, "title_locked")
        self.assertEqual(self.service.equip("stale", "u", ["1"], "1", "1").status, "state_changed")
        self.service.equip("same", "u", ["1", "2"], "1", "2")
        # Request identity is equip+user+title_id; mutable expected_* no longer cause conflict.
        self.assertEqual(self.service.equip("same", "u", ["1", "2"], "2", "2").status, "duplicate")
        self.assertEqual(self.service.equip("same", "u", ["1", "2"], "2", "1").status, "operation_conflict")

    def test_operation_failure_rolls_back(self):
        with db_backend.transaction(self.db) as conn:
            conn.execute("CREATE TABLE title_transaction_operations(operation_id TEXT PRIMARY KEY,payload TEXT,result_status TEXT,title_id TEXT,created_at TEXT)")
            conn.execute("CREATE TRIGGER fail_title_equip BEFORE INSERT ON title_transaction_operations BEGIN SELECT RAISE(ABORT,'failed'); END")
        with self.assertRaises(Exception):
            self.service.equip("fail", "u", ["1", "2"], "1", "2")
        with db_backend.connection(self.db) as conn:
            self.assertEqual(conn.execute("SELECT equipped FROM title").fetchone()[0], "1")
