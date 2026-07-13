import tempfile
import unittest
from pathlib import Path

import nonebot

nonebot.init()

from nonebot_plugin_xiuxian_2.xiuxian.xiuxian_rift.boss_token_service import RiftBossTokenService
from tests.test_db_backend import db_backend


class RiftBossTokenServiceTests(unittest.TestCase):
    def setUp(self):
        self.temp_dir = tempfile.TemporaryDirectory()
        self.database = Path(self.temp_dir.name) / "game.db"
        self.snapshot = {"name": "boss", "rank": 4}
        with db_backend.transaction(self.database) as conn:
            conn.execute("CREATE TABLE user_cd(user_id TEXT PRIMARY KEY,type INTEGER,create_time TEXT,scheduled_time TEXT)")
            conn.execute("CREATE TABLE rift_entries(user_id TEXT PRIMARY KEY,rift_data TEXT,status TEXT)")
            conn.execute("CREATE TABLE back(user_id TEXT,goods_id INTEGER,goods_num INTEGER,UNIQUE(user_id,goods_id))")
            conn.execute("INSERT INTO user_cd VALUES ('u',3,'now','30')")
            conn.execute("INSERT INTO rift_entries VALUES ('u',%s,'active')", ('{"name":"boss","rank":4}',))
            conn.execute("INSERT INTO back VALUES ('u',20018,1)")
        self.service = RiftBossTokenService(self.database)

    def tearDown(self):
        self.temp_dir.cleanup()

    def test_boss_token_claim_is_atomic_and_idempotent(self):
        first = self.service.settle("op", "u", 20018, self.snapshot)
        duplicate = self.service.settle("op", "u", 20018, self.snapshot)
        self.assertEqual((first.status, duplicate.status), ("applied", "duplicate"))
        with db_backend.transaction(self.database) as conn:
            self.assertEqual(conn.execute("SELECT goods_num FROM back").fetchone()[0], 0)
            self.assertEqual(conn.execute("SELECT status FROM rift_entries").fetchone()[0], "settled")
            self.assertEqual(conn.execute("SELECT COUNT(*) FROM rift_boss_token_operations").fetchone()[0], 1)

    def test_changed_snapshot_is_rejected(self):
        result = self.service.settle("op", "u", 20018, {**self.snapshot, "rank": 5})
        self.assertEqual(result.status, "state_changed")
        with db_backend.transaction(self.database) as conn:
            self.assertEqual(conn.execute("SELECT goods_num FROM back").fetchone()[0], 1)
            self.assertEqual(conn.execute("SELECT status FROM rift_entries").fetchone()[0], "active")

    def test_operation_failure_rolls_back(self):
        with db_backend.transaction(self.database) as conn:
            conn.execute("CREATE TABLE rift_boss_token_operations(operation_id TEXT PRIMARY KEY,payload TEXT,created_at TEXT)")
            conn.execute("CREATE TRIGGER fail_operation BEFORE INSERT ON rift_boss_token_operations BEGIN SELECT RAISE(ABORT,'fail'); END")
        with self.assertRaises(Exception):
            self.service.settle("op", "u", 20018, self.snapshot)
        with db_backend.transaction(self.database) as conn:
            self.assertEqual(conn.execute("SELECT goods_num FROM back").fetchone()[0], 1)
            self.assertEqual(conn.execute("SELECT status FROM rift_entries").fetchone()[0], "active")


if __name__ == "__main__":
    unittest.main()
