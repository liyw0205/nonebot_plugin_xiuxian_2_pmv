import tempfile
import unittest
from pathlib import Path

from ..root_repository import AdminRootChangeSqlRepository
from tests.test_db_backend import db_backend


class AdminRootChangeRepositoryTests(unittest.TestCase):
    def test_change_replay_and_snapshot_conflict(self):
        with tempfile.TemporaryDirectory() as temp:
            db = Path(temp) / "game.db"
            with db_backend.transaction(db) as conn:
                conn.execute("CREATE TABLE user_xiuxian(user_id TEXT PRIMARY KEY,root TEXT,root_type TEXT,root_level INTEGER,level TEXT,exp INTEGER,power INTEGER,user_name TEXT)")
                conn.execute("INSERT INTO user_xiuxian VALUES('u','旧根','旧类',1,'筑基',100,10,'道友')")
            repo = AdminRootChangeSqlRepository(db)
            snapshot = ("旧根", "旧类", 1, "筑基", 100, 10, "道友")
            first = repo.change("r1", "admin", "u", snapshot, 8, 2.6, 7.0)
            duplicate = repo.change("r1", "admin", "u", snapshot, 8, 2.6, 7.0)
            stale = repo.change("r2", "admin", "u", snapshot, 8, 2.6, 7.0)
            self.assertEqual((first.status, duplicate.status, stale.status), ("applied", "duplicate", "state_changed"))
