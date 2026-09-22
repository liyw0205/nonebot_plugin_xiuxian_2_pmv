import tempfile
import unittest
from pathlib import Path

from ..settlement_repository import WorkSettlementSqlRepository
from tests.test_db_backend import db_backend


class WorkSettlementRepositoryTests(unittest.TestCase):
    def test_applied_duplicate_and_state_changed(self):
        with tempfile.TemporaryDirectory() as temp:
            db = Path(temp) / "game.db"
            with db_backend.transaction(db) as conn:
                conn.execute("CREATE TABLE user_xiuxian(user_id TEXT PRIMARY KEY,exp INTEGER)")
                conn.execute("INSERT INTO user_xiuxian VALUES('u',90)")
                conn.execute("CREATE TABLE user_cd(user_id TEXT PRIMARY KEY,type INTEGER,create_time TEXT,scheduled_time TEXT)")
                conn.execute("INSERT INTO user_cd VALUES('u',2,'2026-01-01','任务')")
                conn.execute("CREATE TABLE back(user_id TEXT,goods_id INTEGER,goods_name TEXT,goods_type TEXT,goods_num INTEGER,create_time TEXT,update_time TEXT,bind_num INTEGER,UNIQUE(user_id,goods_id))")
            repo = WorkSettlementSqlRepository(db)
            first = repo.settle("s1", "u", {"create_time":"2026-01-01","scheduled_time":"任务"}, 10, {"goods_id":1,"goods_name":"奖励","goods_type":"物品","quantity":2}, 100)
            duplicate = repo.settle("s1", "u", {"create_time":"2026-01-01","scheduled_time":"任务"}, 99, None, 100)
            stale = repo.settle("s2", "u", {"create_time":"old","scheduled_time":"任务"}, 10, None, 100)
            self.assertEqual((first.status, duplicate.status, stale.status), ("applied", "duplicate", "state_changed"))
