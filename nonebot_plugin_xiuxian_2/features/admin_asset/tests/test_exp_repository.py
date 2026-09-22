import tempfile
import unittest
from pathlib import Path

from ..exp_repository import AdminExpAdjustmentSqlRepository
from tests.test_db_backend import db_backend


class AdminExpAdjustmentRepositoryTests(unittest.TestCase):
    def test_adjust_replay_and_snapshot_conflict(self):
        with tempfile.TemporaryDirectory() as temp:
            db = Path(temp) / "game.db"
            with db_backend.transaction(db) as conn:
                conn.execute("CREATE TABLE user_xiuxian(user_id TEXT PRIMARY KEY,exp INTEGER)")
                conn.execute("INSERT INTO user_xiuxian VALUES('u',100)")
            repo = AdminExpAdjustmentSqlRepository(db)
            first = repo.adjust("e1", "admin", "u", 100, 25)
            duplicate = repo.adjust("e1", "admin", "u", 100, 25)
            stale = repo.adjust("e2", "admin", "u", 100, 25)
            self.assertEqual((first.status, duplicate.status, stale.status), ("adjusted", "duplicate", "state_changed"))
