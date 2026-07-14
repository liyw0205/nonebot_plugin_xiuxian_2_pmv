import tempfile
import unittest
from pathlib import Path

import nonebot

nonebot.init()

from nonebot_plugin_xiuxian_2.xiuxian.xiuxian_rift.termination_service import RiftTerminationService
from tests.test_db_backend import db_backend


class RiftTerminationServiceTests(unittest.TestCase):
    def setUp(self):
        self.temp_dir = tempfile.TemporaryDirectory()
        self.database = Path(self.temp_dir.name) / "game.db"
        self.snapshot = {"name": "test", "rank": 2, "time": 30}
        with db_backend.transaction(self.database) as conn:
            conn.execute("CREATE TABLE user_cd(user_id TEXT PRIMARY KEY,type INTEGER,create_time TEXT,scheduled_time TEXT)")
            conn.execute("CREATE TABLE rift_entries(user_id TEXT PRIMARY KEY,rift_data TEXT,status TEXT)")
            conn.execute("INSERT INTO user_cd VALUES ('u',3,'now','30')")
            conn.execute("INSERT INTO rift_entries VALUES ('u',%s,'active')", ('{"name":"test","rank":2,"time":30}',))
        self.service = RiftTerminationService(self.database)

    def tearDown(self):
        self.temp_dir.cleanup()

    def test_terminate_is_atomic_and_idempotent(self):
        first = self.service.terminate("op", "u", self.snapshot)
        duplicate = self.service.terminate("op", "u", self.snapshot)
        replay = self.service.replay("op", "u")
        self.assertEqual((first.status, duplicate.status), ("applied", "duplicate"))
        self.assertEqual(("duplicate", "test"), (replay.status, replay.rift_name))
        with db_backend.transaction(self.database) as conn:
            self.assertEqual(conn.execute("SELECT status FROM rift_entries").fetchone()[0], "terminated")
            self.assertEqual(conn.execute("SELECT type FROM user_cd").fetchone()[0], 0)

    def test_snapshot_change_is_rejected(self):
        result = self.service.terminate("op", "u", {**self.snapshot, "rank": 3})
        self.assertEqual(result.status, "state_changed")
        with db_backend.transaction(self.database) as conn:
            self.assertEqual(conn.execute("SELECT status FROM rift_entries").fetchone()[0], "active")

    def test_operation_failure_rolls_back(self):
        with db_backend.transaction(self.database) as conn:
            conn.execute("CREATE TABLE rift_termination_operations(operation_id TEXT PRIMARY KEY,payload TEXT,created_at TEXT)")
            conn.execute("CREATE TRIGGER fail_operation BEFORE INSERT ON rift_termination_operations BEGIN SELECT RAISE(ABORT,'fail'); END")
        with self.assertRaises(Exception):
            self.service.terminate("op", "u", self.snapshot)
        with db_backend.transaction(self.database) as conn:
            self.assertEqual(conn.execute("SELECT status FROM rift_entries").fetchone()[0], "active")
            self.assertEqual(conn.execute("SELECT type FROM user_cd").fetchone()[0], 3)


if __name__ == "__main__":
    unittest.main()
