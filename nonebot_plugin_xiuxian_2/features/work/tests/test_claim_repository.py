import tempfile
import unittest
from pathlib import Path

from ..claim_repository import WorkClaimSqlRepository
from ..migrations import apply_work_abort_cleanup, apply_work_claim_operations
from ....infrastructure.database import DatabaseUnitOfWork
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
            with DatabaseUnitOfWork(db) as uow:
                apply_work_abort_cleanup(uow)
                apply_work_claim_operations(uow)
            repo = WorkClaimSqlRepository(db)
            offer = {"tasks": {"采药": {"time": 5}}, "status": 1, "refresh_time": "2026-01-01 00:00:00"}
            first = repo.claim("c1", "u", 3, offer, 1, "2026-01-01 00:00:00")
            duplicate = repo.claim("c1", "u", 99, offer, 1, "2099")
            stale = repo.claim("c2", "u", 3, offer, 1, "2026-01-01 00:00:00")
            self.assertEqual((first.status, duplicate.status, stale.status), ("applied", "duplicate", "state_changed"))

    def test_missing_startup_schema_is_not_created_by_request(self):
        with tempfile.TemporaryDirectory() as temp:
            db = Path(temp) / "game.db"
            with db_backend.transaction(db) as conn:
                conn.execute("CREATE TABLE user_xiuxian(user_id TEXT PRIMARY KEY,work_num INTEGER)")
                conn.execute("INSERT INTO user_xiuxian VALUES('u',3)")
                conn.execute("CREATE TABLE user_cd(user_id TEXT PRIMARY KEY,type INTEGER,create_time TEXT,scheduled_time TEXT)")
                conn.execute("INSERT INTO user_cd VALUES('u',0,'0',NULL)")

            result = WorkClaimSqlRepository(db).claim(
                "missing", "u", 3, {"tasks": {"采药": {"time": 5}}}, 1, "now"
            )

            self.assertEqual(result.status, "schema_missing")
            with db_backend.connection(db) as conn:
                tables = {row[0] for row in conn.execute("SELECT name FROM sqlite_master WHERE type='table'")}
            self.assertNotIn("work_claim_operations", tables)
            self.assertNotIn("work_active_snapshots", tables)

    def test_startup_migration_preserves_existing_claim_receipt(self):
        with tempfile.TemporaryDirectory() as temp:
            db = Path(temp) / "game.db"
            with db_backend.transaction(db) as conn:
                conn.execute("CREATE TABLE work_claim_operations(operation_id TEXT PRIMARY KEY,payload TEXT NOT NULL,task_name TEXT NOT NULL,started_at TEXT NOT NULL,remaining_count INTEGER NOT NULL,created_at TEXT)")
                conn.execute("INSERT INTO work_claim_operations VALUES('historic','[\"u\",1]','采药','old',3,'old')")
                conn.execute("CREATE TABLE work_active_snapshots(user_id TEXT PRIMARY KEY,snapshot TEXT NOT NULL,updated_at TEXT NOT NULL)")
            with DatabaseUnitOfWork(db) as uow:
                apply_work_abort_cleanup(uow)
                apply_work_claim_operations(uow)

            replay = WorkClaimSqlRepository(db).claim(
                "historic", "u", 99, {"tasks": {"采药": {"time": 5}}}, 1, "new"
            )

            self.assertEqual((replay.status, replay.task_name, replay.started_at, replay.remaining_count), ("duplicate", "采药", "old", 3))
