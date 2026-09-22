import tempfile
import unittest
from pathlib import Path

from ..item_destroy_repository import AdminItemDestroySqlRepository
from tests.test_db_backend import db_backend


class AdminItemDestroyRepositoryTests(unittest.TestCase):
    def test_destroy_replay_and_snapshot_conflict(self):
        with tempfile.TemporaryDirectory() as temp:
            db = Path(temp) / "game.db"
            with db_backend.transaction(db) as conn:
                conn.execute("CREATE TABLE user_xiuxian(user_id TEXT PRIMARY KEY)")
                conn.execute("INSERT INTO user_xiuxian VALUES('u')")
                conn.execute("CREATE TABLE back(user_id TEXT,goods_id INTEGER,goods_name TEXT,goods_type TEXT,goods_num INTEGER,bind_num INTEGER,update_time TEXT,UNIQUE(user_id,goods_id))")
                conn.execute("INSERT INTO back VALUES('u',10,'丹药','丹药',5,4,'')")
            repo = AdminItemDestroySqlRepository(db)
            first = repo.destroy("d1", "admin", "u", 10, "丹药", "丹药", 2, 5)
            duplicate = repo.destroy("d1", "admin", "u", 10, "丹药", "丹药", 2, 5)
            stale = repo.destroy("d2", "admin", "u", 10, "丹药", "丹药", 2, 5)
            self.assertEqual((first.status, duplicate.status, stale.status), ("destroyed", "duplicate", "state_changed"))
