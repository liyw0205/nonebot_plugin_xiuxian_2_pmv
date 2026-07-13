import tempfile
import unittest
from pathlib import Path

import nonebot
nonebot.init()

from nonebot_plugin_xiuxian_2.xiuxian.xiuxian_work.abort_service import WorkAbortService
from tests.test_db_backend import db_backend


class WorkAbortServiceTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory(); self.db = Path(self.temp.name) / "game.db"
        with db_backend.transaction(self.db) as conn:
            conn.execute("CREATE TABLE user_xiuxian(user_id TEXT PRIMARY KEY,stone INTEGER)")
            conn.execute("CREATE TABLE user_cd(user_id TEXT PRIMARY KEY,type INTEGER,create_time TEXT,scheduled_time TEXT)")
            conn.execute("CREATE TABLE work_active_snapshots(user_id TEXT PRIMARY KEY,snapshot TEXT,updated_at TEXT)")
            conn.execute("INSERT INTO user_xiuxian VALUES (%s,%s)", ("u", 5000000))
            conn.execute("INSERT INTO user_cd VALUES (%s,%s,%s,%s)", ("u",2,"2026-01-01 00:00:00","任务"))
            conn.execute("INSERT INTO work_active_snapshots VALUES (%s,%s,%s)", ("u","{}","now"))
        self.service = WorkAbortService(self.db)
        self.work = {"create_time":"2026-01-01 00:00:00","scheduled_time":"任务"}

    def tearDown(self): self.temp.cleanup()

    def test_success_duplicate_conflict_and_stale(self):
        first = self.service.abort("op", "u", self.work, 5000000, 4000000)
        duplicate = self.service.abort("op", "u", self.work, 5000000, 4000000)
        conflict = self.service.abort("op", "u", self.work, 1000000, 1)
        stale = self.service.abort("stale", "u", self.work, 1000000, 1)
        self.assertEqual((first.status, duplicate.status, conflict.status, stale.status), ("applied","duplicate","operation_conflict","state_changed"))
        with db_backend.connection(self.db) as conn:
            self.assertEqual(tuple(conn.execute("SELECT stone FROM user_xiuxian").fetchone()), (1000000,))
            self.assertEqual(tuple(conn.execute("SELECT type,scheduled_time FROM user_cd").fetchone()), (0,None))
            self.assertEqual(conn.execute("SELECT COUNT(*) FROM work_active_snapshots").fetchone()[0], 0)

    def test_operation_failure_rolls_back(self):
        with db_backend.transaction(self.db) as conn:
            conn.execute("CREATE TABLE work_abort_operations(operation_id TEXT PRIMARY KEY,payload TEXT,penalty INTEGER,stone_remaining INTEGER)")
            conn.execute("CREATE TRIGGER fail_abort BEFORE INSERT ON work_abort_operations BEGIN SELECT RAISE(ABORT,'failed'); END")
        with self.assertRaises(db_backend.IntegrityError): self.service.abort("fail", "u", self.work, 5000000, 4000000)
        with db_backend.connection(self.db) as conn: self.assertEqual(tuple(conn.execute("SELECT stone FROM user_xiuxian").fetchone()), (5000000,))


if __name__ == "__main__": unittest.main()
