from __future__ import annotations

import tempfile
import unittest
from pathlib import Path

import nonebot

nonebot.init()

from nonebot_plugin_xiuxian_2.xiuxian.xiuxian_work.item_use_service import WorkItemUseService
from tests.test_db_backend import db_backend


class WorkItemUseServiceTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.database = Path(self.temp.name) / "game.sqlite3"
        with db_backend.transaction(self.database) as conn:
            conn.execute("CREATE TABLE user_cd(user_id TEXT PRIMARY KEY,type INTEGER,create_time TEXT,scheduled_time TEXT)")
            conn.execute("INSERT INTO user_cd VALUES(%s,%s,%s,%s)", ("u", 2, "2026-07-13 10:00:00", "镇妖"))
            conn.execute("CREATE TABLE back(user_id TEXT,goods_id INTEGER,goods_num INTEGER,bind_num INTEGER,UNIQUE(user_id,goods_id))")
            conn.execute("INSERT INTO back VALUES(%s,%s,%s,%s)", ("u", 20014, 2, 1))
            conn.execute("INSERT INTO back VALUES(%s,%s,%s,%s)", ("u", 20015, 2, 0))
        self.service = WorkItemUseService(self.database)
        self.work = {"type": 2, "create_time": "2026-07-13 10:00:00", "scheduled_time": "镇妖"}
        self.offer = {"tasks": {"采药": {"award": 30}}, "status": 1, "refresh_time": "2026-07-13 11:00:00"}

    def tearDown(self):
        self.temp.cleanup()

    def test_accelerate_consumes_item_and_advances_work(self):
        result = self.service.accelerate("speed", "u", 20014, 2, self.work, "1970-01-01 00:00:00")
        self.assertEqual((result.status, result.item_remaining), ("applied", 1))
        with db_backend.connection(self.database) as conn:
            work = conn.execute("SELECT create_time FROM user_cd WHERE user_id=%s", ("u",)).fetchone()[0]
            item = conn.execute("SELECT goods_num,bind_num FROM back WHERE goods_id=%s", (20014,)).fetchone()
        self.assertEqual((work, tuple(item)), ("1970-01-01 00:00:00", (1, 0)))

    def test_capture_consumes_item_and_records_fixed_offer(self):
        with db_backend.transaction(self.database) as conn:
            conn.execute("UPDATE user_cd SET type=0,create_time='0',scheduled_time=NULL WHERE user_id=%s", ("u",))
        result = self.service.capture("capture", "u", 20015, 2, 0, self.offer)
        self.assertEqual(result.status, "applied")
        with db_backend.connection(self.database) as conn:
            snapshot = conn.execute("SELECT snapshot FROM work_offer_snapshots WHERE user_id=%s", ("u",)).fetchone()[0]
            count = conn.execute("SELECT goods_num FROM back WHERE goods_id=%s", (20015,)).fetchone()[0]
        self.assertIn('"award": 30', snapshot)
        self.assertEqual(count, 1)

    def test_duplicate_conflict_and_changed_snapshot(self):
        self.assertEqual(self.service.accelerate("same", "u", 20014, 2, self.work, "1970-01-01 00:00:00").status, "applied")
        self.assertEqual(self.service.accelerate("same", "u", 20014, 2, self.work, "1970-01-01 00:00:00").status, "duplicate")
        self.assertEqual(self.service.accelerate("same", "u", 20014, 2, self.work, "1969-01-01 00:00:00").status, "operation_conflict")
        self.assertEqual(self.service.accelerate("stale", "u", 20014, 2, self.work, "1970-01-01 00:00:00").status, "state_changed")

    def test_operation_failure_rolls_back_item_and_work(self):
        with db_backend.transaction(self.database) as conn:
            conn.execute("CREATE TABLE work_item_use_operations(operation_id TEXT PRIMARY KEY,payload TEXT,action TEXT,item_remaining INTEGER,result_snapshot TEXT,created_at TEXT)")
            conn.execute("CREATE TRIGGER fail_item_use BEFORE INSERT ON work_item_use_operations BEGIN SELECT RAISE(ABORT,'fail'); END")
        with self.assertRaises(db_backend.IntegrityError):
            self.service.accelerate("rollback", "u", 20014, 2, self.work, "1970-01-01 00:00:00")
        with db_backend.connection(self.database) as conn:
            work = conn.execute("SELECT create_time FROM user_cd WHERE user_id=%s", ("u",)).fetchone()[0]
            count = conn.execute("SELECT goods_num FROM back WHERE goods_id=%s", (20014,)).fetchone()[0]
        self.assertEqual((work, count), ("2026-07-13 10:00:00", 2))


if __name__ == "__main__":
    unittest.main()
