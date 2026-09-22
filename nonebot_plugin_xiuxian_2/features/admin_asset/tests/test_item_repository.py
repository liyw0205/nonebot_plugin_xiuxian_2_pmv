import tempfile
import unittest
from pathlib import Path

from ..item_repository import AdminItemSqlRepository
from tests.test_db_backend import db_backend


class AdminItemRepositoryTests(unittest.TestCase):
    def test_applied_duplicate_and_state_changed(self):
        with tempfile.TemporaryDirectory() as temp:
            db = Path(temp) / "game.db"
            with db_backend.transaction(db) as conn:
                conn.execute("CREATE TABLE back(user_id TEXT,goods_id INTEGER,goods_name TEXT,goods_type TEXT,goods_num INTEGER,bind_num INTEGER,PRIMARY KEY(user_id,goods_id))")
                conn.execute("INSERT INTO back VALUES('u',1,'旧','物品',2,0)")
            repo = AdminItemSqlRepository(db)
            first = repo.grant("i1", "op", "u", 1, "物品", "物品", 3, 2, 99)
            duplicate = repo.grant("i1", "op", "u", 1, "物品", "物品", 3, 999, 99)
            stale = repo.grant("i2", "op", "u", 1, "物品", "物品", 3, 2, 99)
            self.assertEqual((first.status, duplicate.status, stale.status), ("granted", "duplicate", "state_changed"))
