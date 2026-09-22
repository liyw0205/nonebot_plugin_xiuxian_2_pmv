import tempfile
import unittest
from pathlib import Path

from ..stone_repository import AdminStoneSqlRepository
from tests.test_db_backend import db_backend


class AdminStoneRepositoryTests(unittest.TestCase):
    def test_applied_duplicate_and_state_changed(self):
        with tempfile.TemporaryDirectory() as temp:
            db = Path(temp) / "game.db"
            with db_backend.transaction(db) as conn:
                conn.execute("CREATE TABLE user_xiuxian(user_id TEXT PRIMARY KEY,stone INTEGER)")
                conn.execute("INSERT INTO user_xiuxian VALUES('u',100)")
            repo = AdminStoneSqlRepository(db)
            first = repo.adjust("a1", "op", "u", 100, 10)
            duplicate = repo.adjust("a1", "op", "u", 999, 10)
            stale = repo.adjust("a2", "op", "u", 100, 10)
            self.assertEqual((first.status, duplicate.status, stale.status), ("applied", "duplicate", "state_changed"))
