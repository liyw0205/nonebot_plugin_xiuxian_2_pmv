import tempfile
import unittest
from pathlib import Path

from ..speedup_repository import RiftSpeedupSqlRepository
from tests.test_db_backend import db_backend


class RiftSpeedupRepositoryTests(unittest.TestCase):
    def test_applied_duplicate_and_item_missing(self):
        with tempfile.TemporaryDirectory() as temp:
            database = Path(temp) / "game.db"
            with db_backend.transaction(database) as conn:
                conn.execute("CREATE TABLE rift_entries(user_id TEXT PRIMARY KEY,rift_data TEXT,status TEXT,duration INTEGER)")
                conn.execute("CREATE TABLE user_cd(user_id TEXT PRIMARY KEY,type INTEGER,create_time TEXT,scheduled_time INTEGER)")
                conn.execute("CREATE TABLE back(user_id TEXT,goods_id INTEGER,goods_num INTEGER,bind_num INTEGER,PRIMARY KEY(user_id,goods_id))")
                conn.execute("INSERT INTO rift_entries VALUES('u','{\"time\":100}','active',100)")
                conn.execute("INSERT INTO user_cd VALUES('u',3,'now',100)")
                conn.execute("INSERT INTO back VALUES('u',9,1,1)")
            repo = RiftSpeedupSqlRepository(database)
            first = repo.apply("r1", "u", 9, {"time": 100}, {"type": 3, "create_time": "now", "scheduled_time": 100}, 50)
            duplicate = repo.apply("r1", "u", 9, {"time": 100}, {"type": 3, "create_time": "now", "scheduled_time": 100}, 50)
            missing = repo.apply("r2", "u", 9, {"time": 50}, {"type": 3, "create_time": "now", "scheduled_time": 50}, 50)
            self.assertEqual((first.status, duplicate.status, missing.status), ("applied", "duplicate", "item_missing"))
