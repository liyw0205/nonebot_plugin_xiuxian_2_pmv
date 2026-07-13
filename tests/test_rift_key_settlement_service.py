import tempfile
import unittest
from pathlib import Path

import nonebot

nonebot.init()

from nonebot_plugin_xiuxian_2.xiuxian.xiuxian_rift.key_settlement_service import RiftKeySettlementService
from tests.test_db_backend import db_backend


class RiftKeySettlementServiceTests(unittest.TestCase):
    def setUp(self):
        self.temp_dir = tempfile.TemporaryDirectory()
        self.database = Path(self.temp_dir.name) / "game.db"
        self.snapshot = {"name": "test", "rank": 2}
        with db_backend.transaction(self.database) as conn:
            conn.execute("CREATE TABLE user_cd(user_id TEXT PRIMARY KEY,type INTEGER,create_time TEXT,scheduled_time TEXT)")
            conn.execute("CREATE TABLE rift_entries(user_id TEXT PRIMARY KEY,rift_data TEXT,status TEXT)")
            conn.execute("CREATE TABLE back(user_id TEXT,goods_id INTEGER,goods_num INTEGER,UNIQUE(user_id,goods_id))")
            conn.execute("INSERT INTO user_cd VALUES ('u',3,'now','30')")
            conn.execute("INSERT INTO rift_entries VALUES ('u',%s,'active')", ('{"name":"test","rank":2}',))
            conn.execute("INSERT INTO back VALUES ('u',20001,1)")
        self.service = RiftKeySettlementService(self.database)

    def tearDown(self):
        self.temp_dir.cleanup()

    def test_key_consumption_and_claim_are_atomic(self):
        first = self.service.settle("op", "u", 20001, self.snapshot)
        duplicate = self.service.settle("op", "u", 20001, self.snapshot)
        self.assertEqual((first.status, duplicate.status), ("applied", "duplicate"))
        with db_backend.transaction(self.database) as conn:
            self.assertEqual(conn.execute("SELECT goods_num FROM back").fetchone()[0], 0)
            self.assertEqual(conn.execute("SELECT status FROM rift_entries").fetchone()[0], "settled")
            self.assertEqual(conn.execute("SELECT type FROM user_cd").fetchone()[0], 0)

    def test_missing_key_changes_nothing(self):
        with db_backend.transaction(self.database) as conn:
            conn.execute("UPDATE back SET goods_num=0")
        self.assertEqual(self.service.settle("op", "u", 20001, self.snapshot).status, "item_missing")
        with db_backend.transaction(self.database) as conn:
            self.assertEqual(conn.execute("SELECT status FROM rift_entries").fetchone()[0], "active")
            self.assertEqual(conn.execute("SELECT type FROM user_cd").fetchone()[0], 3)

    def test_operation_failure_rolls_back(self):
        with db_backend.transaction(self.database) as conn:
            conn.execute("CREATE TABLE rift_key_operations(operation_id TEXT PRIMARY KEY,payload TEXT,created_at TEXT)")
            conn.execute("CREATE TRIGGER fail_operation BEFORE INSERT ON rift_key_operations BEGIN SELECT RAISE(ABORT,'fail'); END")
        with self.assertRaises(Exception):
            self.service.settle("op", "u", 20001, self.snapshot)
        with db_backend.transaction(self.database) as conn:
            self.assertEqual(conn.execute("SELECT goods_num FROM back").fetchone()[0], 1)
            self.assertEqual(conn.execute("SELECT status FROM rift_entries").fetchone()[0], "active")


if __name__ == "__main__":
    unittest.main()
