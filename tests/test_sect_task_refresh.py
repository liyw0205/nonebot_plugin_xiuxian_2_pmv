import tempfile
import unittest
from pathlib import Path

import nonebot
nonebot.init()

from nonebot_plugin_xiuxian_2.xiuxian.xiuxian_sect.membership_service import SectMembershipService
from tests.test_db_backend import db_backend


class SectTaskRefreshTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.db = Path(self.temp.name) / "sect.db"
        with db_backend.transaction(self.db) as conn:
            conn.execute("CREATE TABLE user_xiuxian (user_id TEXT PRIMARY KEY,sect_id INTEGER,sect_task INTEGER)")
            conn.execute("CREATE TABLE sects (sect_id INTEGER PRIMARY KEY)")
            conn.execute("CREATE TABLE sect_task_state (user_id TEXT,sect_id INTEGER,task_key TEXT,task_data TEXT,period TEXT,status TEXT,progress INTEGER,target INTEGER,accepted_at TEXT,updated_at TEXT,completed_at TEXT,PRIMARY KEY(user_id,period))")
            conn.execute("INSERT INTO user_xiuxian VALUES (%s,%s,%s)", ("u",1,0))
            conn.execute("INSERT INTO sects VALUES (%s)", (1,))
            conn.execute("INSERT INTO sect_task_state VALUES (%s,%s,%s,%s,%s,%s,0,1,'','',NULL)", ("u",1,"旧任务",'{"type": 1}',"2026-07-13","accepted"))
        self.service = SectMembershipService(self.db)

    def tearDown(self): self.temp.cleanup()

    def state(self):
        with db_backend.connection(self.db) as conn:
            return tuple(conn.execute("SELECT task_key,task_data,status FROM sect_task_state WHERE user_id=%s", ("u",)).fetchone())

    def test_refresh_and_duplicate(self):
        first = self.service.refresh_task("op","u",1,"2026-07-13","旧任务",{"type":1},"新任务",{"type":2},3)
        duplicate = self.service.refresh_task("op","u",1,"2026-07-13","其他",{},"其他",{},3)
        self.assertEqual((first.status,duplicate.status,duplicate.task_key), ("claimed","duplicate","新任务"))
        self.assertEqual(self.state(), ("新任务",'{"type": 2}',"accepted"))

    def test_stale_and_missing_rejected(self):
        self.assertEqual(self.service.refresh_task("stale","u",1,"2026-07-13","错",{},"新",{},3).status,"state_changed")
        with db_backend.transaction(self.db) as conn: conn.execute("DELETE FROM sect_task_state")
        self.assertEqual(self.service.refresh_task("missing","u",1,"2026-07-13","旧任务",{"type":1},"新",{},3).status,"task_missing")

    def test_failure_rolls_back(self):
        with db_backend.transaction(self.db) as conn:
            self.service._ensure_task_claim_operations(conn)
            conn.execute("CREATE TRIGGER fail_refresh BEFORE INSERT ON sect_task_claim_operations BEGIN SELECT RAISE(ABORT,'failed'); END")
        with self.assertRaises(db_backend.IntegrityError):
            self.service.refresh_task("fail","u",1,"2026-07-13","旧任务",{"type":1},"新",{},3)
        self.assertEqual(self.state(), ("旧任务",'{"type": 1}',"accepted"))


if __name__ == '__main__': unittest.main()
