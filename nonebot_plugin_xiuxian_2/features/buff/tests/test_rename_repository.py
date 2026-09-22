import tempfile
import unittest
from pathlib import Path

from ..rename_repository import BlessedSpotRenameSqlRepository
from tests.test_db_backend import db_backend


class BlessedSpotRenameRepositoryTests(unittest.TestCase):
    def test_applied_and_duplicate(self):
        with tempfile.TemporaryDirectory() as temp:
            db = Path(temp) / "game.db"
            with db_backend.transaction(db) as conn:
                conn.execute("CREATE TABLE user_xiuxian(user_id TEXT PRIMARY KEY,blessed_spot_flag INTEGER,blessed_spot_name TEXT)")
                conn.execute("INSERT INTO user_xiuxian VALUES('u',1,'旧名')")
            repository = BlessedSpotRenameSqlRepository(db)
            first = repository.rename("r1", "u", "旧名", "新名")
            duplicate = repository.rename("r1", "u", "旧名", "新名")
            self.assertEqual((first.status, duplicate.status), ("applied", "duplicate"))

    def test_state_and_missing_are_rejected(self):
        with tempfile.TemporaryDirectory() as temp:
            db = Path(temp) / "game.db"
            with db_backend.transaction(db) as conn:
                conn.execute("CREATE TABLE user_xiuxian(user_id TEXT PRIMARY KEY,blessed_spot_flag INTEGER,blessed_spot_name TEXT)")
                conn.executemany("INSERT INTO user_xiuxian VALUES(?,?,?)", (("u",1,"旧名"),("missing",0,"")))
            repository = BlessedSpotRenameSqlRepository(db)
            self.assertEqual(repository.rename("r2", "u", "错误", "新名").status, "state_changed")
            self.assertEqual(repository.rename("r3", "missing", "", "新名").status, "blessed_spot_missing")
