import tempfile
import unittest
from pathlib import Path

from ..item_repository import AdminItemSqlRepository
from ..migrations import apply_admin_item_grant
from ....infrastructure.database import DatabaseUnitOfWork
from tests.test_db_backend import db_backend


class AdminItemRepositoryTests(unittest.TestCase):
    def test_applied_duplicate_and_state_changed(self):
        with tempfile.TemporaryDirectory() as temp:
            db = Path(temp) / "game.db"
            with db_backend.transaction(db) as conn:
                conn.execute("CREATE TABLE back(user_id TEXT,goods_id INTEGER,goods_name TEXT,goods_type TEXT,goods_num INTEGER,bind_num INTEGER,PRIMARY KEY(user_id,goods_id))")
                conn.execute("INSERT INTO back VALUES('u',1,'旧','物品',2,0)")
            with DatabaseUnitOfWork(db) as uow:
                apply_admin_item_grant(uow)
            repo = AdminItemSqlRepository(db)
            first = repo.grant("i1", "op", "u", 1, "物品", "物品", 3, 2, 99)
            duplicate = repo.grant("i1", "op", "u", 1, "物品", "物品", 3, 999, 99)
            stale = repo.grant("i2", "op", "u", 1, "物品", "物品", 3, 2, 99)
            self.assertEqual((first.status, duplicate.status, stale.status), ("granted", "duplicate", "state_changed"))

    def test_missing_startup_migration_fails_closed_without_request_ddl(self):
        with tempfile.TemporaryDirectory() as temp:
            db = Path(temp) / "game.db"
            with db_backend.transaction(db) as conn:
                conn.execute("CREATE TABLE back(user_id TEXT,goods_id INTEGER,goods_name TEXT,goods_type TEXT,goods_num INTEGER,bind_num INTEGER,PRIMARY KEY(user_id,goods_id))")
                conn.execute("INSERT INTO back VALUES('u',1,'旧','物品',2,0)")

            result = AdminItemSqlRepository(db).grant("i1", "op", "u", 1, "物品", "物品", 3, 2, 99)
            self.assertEqual(result.status, "schema_missing")
            with db_backend.transaction(db) as conn:
                self.assertIsNone(
                    conn.execute(
                        "SELECT 1 FROM sqlite_master WHERE type='table' AND name='admin_item_grant_operations'"
                    ).fetchone()
                )
                self.assertEqual(conn.execute("SELECT goods_num FROM back WHERE user_id='u'").fetchone()[0], 2)
