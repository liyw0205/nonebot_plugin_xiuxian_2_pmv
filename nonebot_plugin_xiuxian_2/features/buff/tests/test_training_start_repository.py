import tempfile
import unittest
from pathlib import Path

from ..training_start_repository import NormalTrainingStartSqlRepository
from tests.test_db_backend import db_backend


class NormalTrainingStartRepositoryTests(unittest.TestCase):
    def test_start_and_replay(self):
        with tempfile.TemporaryDirectory() as temp:
            database = Path(temp) / "game.db"
            with db_backend.transaction(database) as conn:
                conn.execute("CREATE TABLE user_xiuxian(user_id TEXT PRIMARY KEY,exp INTEGER,stone INTEGER)")
                conn.execute("INSERT INTO user_xiuxian VALUES('u',10,20)")
                conn.execute("CREATE TABLE user_cd(user_id TEXT PRIMARY KEY,type INTEGER,create_time TEXT,scheduled_time TEXT)")
                conn.execute("INSERT INTO user_cd VALUES('u',0,NULL,NULL)")
            repo = NormalTrainingStartSqlRepository(database)
            first = repo.start("t1", "u", "cultivation", 10, 20, 5, 100, 1.2)
            duplicate = repo.start("t1", "u", "cultivation", 10, 20, 5, 100, 1.2)
            self.assertEqual((first.status, duplicate.status), ("started", "duplicate"))
