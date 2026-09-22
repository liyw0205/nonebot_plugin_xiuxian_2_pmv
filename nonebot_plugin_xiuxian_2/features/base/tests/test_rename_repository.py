import tempfile
import unittest
from pathlib import Path

from ..rename_repository import BaseRenameSqlRepository
from tests.test_db_backend import db_backend


class BaseRenameRepositoryTests(unittest.TestCase):
    def test_user_rename_applies_and_replays(self):
        with tempfile.TemporaryDirectory() as temp:
            database = Path(temp) / "game.db"
            with db_backend.transaction(database) as conn:
                conn.execute("CREATE TABLE user_xiuxian(user_id TEXT PRIMARY KEY,user_name TEXT,stone INTEGER)")
                conn.execute("INSERT INTO user_xiuxian VALUES('u','旧名',100)")
                conn.execute("CREATE TABLE back(user_id TEXT,goods_id INTEGER,goods_num INTEGER,bind_num INTEGER,PRIMARY KEY(user_id,goods_id))")
            repo = BaseRenameSqlRepository(database)
            first = repo.rename("r1", "u", "user", "新名", stone_cost=10)
            duplicate = repo.rename("r1", "u", "user", "新名", stone_cost=10)
            self.assertEqual((first.status, duplicate.status), ("renamed", "duplicate"))
