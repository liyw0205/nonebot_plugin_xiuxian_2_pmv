import tempfile
import unittest
from pathlib import Path

from ..impart_stone_repository import AdminImpartStoneSqlRepository
from tests.test_db_backend import db_backend


class AdminImpartStoneRepositoryTests(unittest.TestCase):
    def test_missing_user_is_rejected(self):
        with tempfile.TemporaryDirectory() as temp:
            game = Path(temp) / "game.db"
            impart = Path(temp) / "impart.db"
            with db_backend.transaction(game) as conn:
                conn.execute("CREATE TABLE user_xiuxian(user_id TEXT PRIMARY KEY,stone INTEGER)")
            with db_backend.transaction(impart) as conn:
                conn.execute("CREATE TABLE statistics(user_id TEXT PRIMARY KEY,stone INTEGER)")
            result = AdminImpartStoneSqlRepository(game, impart).adjust("i1", "op", "u", 0, 5)
            self.assertEqual(result.status, "user_missing")
