import tempfile
import unittest
from pathlib import Path

import nonebot

nonebot.init()

from nonebot_plugin_xiuxian_2.xiuxian.xiuxian_activity.task_claim_service import ActivityTaskClaimService
from tests.test_db_backend import db_backend


class ActivityTaskClaimTransactionTests(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        root = Path(self.tmp.name)
        self.activity, self.game = root / "activity.db", root / "game.db"
        with db_backend.transaction(self.activity) as conn:
            conn.execute("CREATE TABLE activity_task_progress(activity_key TEXT,user_id TEXT,scope_type TEXT,scope_key TEXT,task_key TEXT,progress INTEGER,target INTEGER,claimed INTEGER,claim_time TEXT,update_time TEXT,PRIMARY KEY(activity_key,user_id,scope_type,scope_key,task_key))")
            conn.execute("CREATE TABLE activity_task_claim_log(id INTEGER PRIMARY KEY,activity_key TEXT,user_id TEXT,scope_type TEXT,scope_key TEXT,task_key TEXT,reward TEXT,create_time TEXT)")
            conn.execute("INSERT INTO activity_task_progress VALUES('a','u','daily','d','t',2,2,0,'','')")
        with db_backend.transaction(self.game) as conn:
            conn.execute("CREATE TABLE user_xiuxian(user_id TEXT PRIMARY KEY,stone INTEGER)")
            conn.execute("INSERT INTO user_xiuxian VALUES('u',10)")
            conn.execute("CREATE TABLE back(user_id TEXT,goods_id INTEGER,goods_name TEXT,goods_type TEXT,goods_num INTEGER,create_time TEXT,update_time TEXT,bind_num INTEGER,UNIQUE(user_id,goods_id))")
        self.service = ActivityTaskClaimService(self.activity, self.game)
        rewards = ({"type": "stone", "quantity": 50}, {"id": 101, "name": "任务令", "type": "道具", "quantity": 2})
        self.tasks = (("t", "daily", "d", 2, "灵石x50,任务令x2", rewards, "每日任务"),)

    def tearDown(self):
        self.tmp.cleanup()

    def test_atomic_claim_and_idempotency(self):
        result = self.service.claim("op", "u", "a", self.tasks, 100)
        self.assertEqual("applied", result.status)
        self.assertEqual("duplicate", self.service.claim("op", "u", "a", self.tasks, 100).status)
        self.assertEqual("duplicate", self.service.get_result("op", "u").status)
        self.assertEqual("operation_conflict", self.service.get_result("op", "other").status)
        with db_backend.connection(self.activity) as conn:
            self.assertEqual((1, 1), tuple(conn.execute("SELECT p.claimed,COUNT(l.id) FROM activity_task_progress p JOIN activity_task_claim_log l ON 1=1").fetchone()))
        with db_backend.connection(self.game) as conn:
            self.assertEqual(60, conn.execute("SELECT stone FROM user_xiuxian").fetchone()[0])
            self.assertEqual(2, conn.execute("SELECT goods_num FROM back").fetchone()[0])

    def test_inventory_and_rollback(self):
        self.assertEqual("inventory_full", self.service.claim("full", "u", "a", self.tasks, 1).status)
        with db_backend.transaction(self.activity) as conn:
            conn.execute("CREATE TABLE activity_task_claim_operations(operation_id TEXT PRIMARY KEY,payload TEXT,result_json TEXT,created_at TEXT)")
            conn.execute("CREATE TRIGGER fail_task BEFORE INSERT ON activity_task_claim_operations BEGIN SELECT RAISE(ABORT,'x'); END")
        with self.assertRaises(Exception):
            self.service.claim("rollback", "u", "a", self.tasks, 100)
        with db_backend.connection(self.activity) as conn:
            self.assertEqual(0, conn.execute("SELECT claimed FROM activity_task_progress").fetchone()[0])
        with db_backend.connection(self.game) as conn:
            self.assertEqual(10, conn.execute("SELECT stone FROM user_xiuxian").fetchone()[0])


if __name__ == "__main__":
    unittest.main()
