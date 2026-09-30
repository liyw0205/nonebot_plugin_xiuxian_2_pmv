import tempfile
import unittest
from pathlib import Path

from ..migrations import apply_admin_stone_adjustment
from ..stone_repository import AdminStoneSqlRepository
from tests.test_db_backend import db_backend


class AdminStoneRepositoryTests(unittest.TestCase):
    def _database(self, directory: str) -> Path:
        db = Path(directory) / "game.db"
        with db_backend.transaction(db) as conn:
            conn.execute("CREATE TABLE user_xiuxian(user_id TEXT PRIMARY KEY,stone INTEGER)")
            conn.execute("INSERT INTO user_xiuxian VALUES('u',100)")
        from ....infrastructure.database import DatabaseUnitOfWork

        with DatabaseUnitOfWork(db) as uow:
            apply_admin_stone_adjustment(uow)
        return db

    def test_applied_duplicate_conflict_and_economy_audit(self):
        with tempfile.TemporaryDirectory() as temp:
            db = self._database(temp)
            repo = AdminStoneSqlRepository(db)
            first = repo.adjust("a1", "op", "u", 100, 10)
            duplicate = repo.adjust("a1", "op", "u", 999, 10)
            conflict = repo.adjust("a1", "op", "u", 100, 20)
            stale = repo.adjust("a2", "op", "u", 100, 10)
            self.assertEqual(
                (first.status, duplicate.status, conflict.status, stale.status),
                ("adjusted", "duplicate", "operation_conflict", "state_changed"),
            )
            with db_backend.connection(db) as conn:
                self.assertEqual(conn.execute("SELECT stone FROM user_xiuxian WHERE user_id='u'").fetchone()[0], 110)
                audit = conn.execute(
                    "SELECT source,action,stone_delta,trace_id,detail FROM economy_log"
                ).fetchone()
                self.assertEqual(tuple(audit[:4]), ("admin", "admin_stone_add", 10, "a1"))
                self.assertIn('"target_name":""', audit[4])
                self.assertEqual(
                    conn.execute("SELECT COUNT(*) FROM admin_stone_adjustment_operations").fetchone()[0],
                    1,
                )

    def test_subtraction_clamps_and_late_failure_rolls_back(self):
        with tempfile.TemporaryDirectory() as temp:
            db = self._database(temp)
            repo = AdminStoneSqlRepository(db)
            result = repo.adjust("subtract", "op", "u", 100, -140, target_name="目标")
            self.assertEqual((result.final_stone, result.applied_delta), (0, -100))
            with db_backend.transaction(db) as conn:
                audit = conn.execute("SELECT action,stone_delta FROM economy_log").fetchone()
                self.assertEqual(tuple(audit), ("admin_stone_cost", -100))
                conn.execute(
                    "CREATE TRIGGER reject_admin_stone BEFORE INSERT ON admin_stone_adjustment_operations "
                    "BEGIN SELECT RAISE(ABORT,'reject admin stone'); END"
                )
            with self.assertRaisesRegex(Exception, "reject admin stone"):
                repo.adjust("late-failure", "op", "u", 0, 5)
            with db_backend.connection(db) as conn:
                self.assertEqual(conn.execute("SELECT stone FROM user_xiuxian WHERE user_id='u'").fetchone()[0], 0)
                self.assertEqual(conn.execute("SELECT COUNT(*) FROM economy_log").fetchone()[0], 1)

    def test_missing_startup_schema_fails_closed_without_request_ddl(self):
        with tempfile.TemporaryDirectory() as temp:
            db = Path(temp) / "game.db"
            with db_backend.transaction(db) as conn:
                conn.execute("CREATE TABLE user_xiuxian(user_id TEXT PRIMARY KEY,stone INTEGER)")
                conn.execute("INSERT INTO user_xiuxian VALUES('u',100)")
            result = AdminStoneSqlRepository(db).adjust("a1", "op", "u", 100, 10)
            self.assertEqual(result.status, "not_ready")
            with db_backend.connection(db) as conn:
                tables = {
                    row[0]
                    for row in conn.execute("SELECT name FROM sqlite_master WHERE type='table'").fetchall()
                }
            self.assertNotIn("admin_stone_adjustment_operations", tables)
            self.assertNotIn("economy_log", tables)
