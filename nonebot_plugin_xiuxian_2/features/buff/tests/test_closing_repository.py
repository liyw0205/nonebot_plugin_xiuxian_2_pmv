import tempfile
import unittest
from pathlib import Path

from ..closing_repository import ClosingSettlementSqlRepository
from tests.test_db_backend import db_backend


class ClosingSettlementRepositoryTests(unittest.TestCase):
    def test_missing_user_is_stable(self):
        with tempfile.TemporaryDirectory() as temp:
            db = Path(temp) / "game.db"
            with db_backend.transaction(db) as conn:
                conn.execute("CREATE TABLE user_xiuxian(user_id TEXT PRIMARY KEY,stone INTEGER)")
                conn.execute("CREATE TABLE user_cd(user_id TEXT PRIMARY KEY,type INTEGER,create_time TEXT)")
            result = ClosingSettlementSqlRepository(db).settle("c1", "missing", "now", 1, 1, 1, 1, 1, 1)
            self.assertEqual(result.status, "user_missing")
