import tempfile
import unittest
from pathlib import Path

from ..settlement_repository import MixelixirSettlementSqlRepository
from tests.test_db_backend import db_backend


class MixelixirSettlementRepositoryTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.db = Path(self.temp.name) / "game.db"
        with db_backend.transaction(self.db) as conn:
            conn.execute("CREATE TABLE user_xiuxian(user_id TEXT PRIMARY KEY,mixelixir_num INTEGER)")
            conn.execute("INSERT INTO user_xiuxian VALUES('u',0)")
            conn.execute("CREATE TABLE back(user_id TEXT,goods_id INTEGER,goods_name TEXT,goods_type TEXT,goods_num INTEGER,bind_num INTEGER,PRIMARY KEY(user_id,goods_id))")
            conn.execute("INSERT INTO back VALUES('u',1,'主药','药材',5,0)")

    def tearDown(self):
        self.temp.cleanup()

    def test_applied_duplicate_and_insufficient(self):
        repo = MixelixirSettlementSqlRepository(self.db)
        first = repo.settle("s1", "u", {1: 2}, 200, "丹药", 1, max_goods_num=99)
        duplicate = repo.settle("s1", "u", {1: 2}, 200, "其他", 9, max_goods_num=99)
        insufficient = repo.settle("s2", "u", {1: 99}, 200, "丹药", 1, max_goods_num=99)
        self.assertEqual((first.status, duplicate.status, insufficient.status), ("applied", "state_changed", "item_insufficient"))
