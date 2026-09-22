import tempfile
import unittest
from pathlib import Path

from ..level_repository import AdminLevelChangeSqlRepository
from tests.test_db_backend import db_backend


class AdminLevelChangeRepositoryTests(unittest.TestCase):
    def test_change_replay_and_snapshot_conflict(self):
        with tempfile.TemporaryDirectory() as temp:
            db = Path(temp) / "game.db"
            with db_backend.transaction(db) as conn:
                conn.execute("CREATE TABLE user_xiuxian(user_id TEXT PRIMARY KEY,level TEXT,exp INTEGER,hp INTEGER,mp INTEGER,atk INTEGER,power INTEGER,root_type TEXT,root_level INTEGER)")
                conn.execute("INSERT INTO user_xiuxian VALUES('u','练气',10,2,3,1,4,'金',1)")
            repo = AdminLevelChangeSqlRepository(db)
            snapshot = ("练气",10,2,3,1,4,"金",1)
            first = repo.change("l1", "admin", "u", snapshot, "筑基", 20, 2, 1.5)
            duplicate = repo.change("l1", "admin", "u", snapshot, "筑基", 20, 2, 1.5)
            stale = repo.change("l2", "admin", "u", snapshot, "筑基", 20, 2, 1.5)
            self.assertEqual((first.status, duplicate.status, stale.status), ("applied", "duplicate", "state_changed"))
