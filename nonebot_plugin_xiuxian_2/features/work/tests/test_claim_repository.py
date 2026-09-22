import tempfile
import unittest
from pathlib import Path

from ..claim_repository import WorkClaimSqlRepository
from tests.test_db_backend import db_backend


class WorkClaimRepositoryTests(unittest.TestCase):
    def test_applied_duplicate_and_state_changed(self):
        with tempfile.TemporaryDirectory() as temp:
            db = Path(temp) / "game.db"
            with db_backend.transaction(db) as conn:
                conn.execute("CREATE TABLE user_xiuxian(user_id TEXT PRIMARY KEY,work_num INTEGER)")
                conn.execute("INSERT INTO user_xiuxian VALUES('u',3)")
                conn.execute("CREATE TABLE user_cd(user_id TEXT PRIMARY KEY,type INTEGER,create_time TEXT,scheduled_time TEXT)")
                conn.execute("INSERT INTO user_cd VALUES('u',0,'0',NULL)")
            repo = WorkClaimSqlRepository(db)
            offer = {"tasks": {"采药": {"time": 5}}, "status": 1, "refresh_time": "2026-01-01 00:00:00"}
            first = repo.claim("c1", "u", 3, offer, 1, "2026-01-01 00:00:00")
            duplicate = repo.claim("c1", "u", 99, offer, 1, "2099")
            stale = repo.claim("c2", "u", 3, offer, 1, "2026-01-01 00:00:00")
            self.assertEqual((first.status, duplicate.status, stale.status), ("applied", "duplicate", "state_changed"))
