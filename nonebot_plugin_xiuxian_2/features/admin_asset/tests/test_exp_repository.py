import sqlite3
import tempfile
import unittest
from pathlib import Path

from ..migrations import apply_admin_exp_adjustment, apply_admin_exp_batch_adjustment, apply_admin_stone_adjustment
from ..exp_repository import AdminExpAdjustmentSqlRepository
from ....infrastructure.database import DatabaseUnitOfWork
from tests.test_db_backend import db_backend


class AdminExpAdjustmentRepositoryTests(unittest.TestCase):
    def test_adjust_replay_and_snapshot_conflict(self):
        with tempfile.TemporaryDirectory() as temp:
            db = Path(temp) / "game.db"
            with db_backend.transaction(db) as conn:
                conn.execute("CREATE TABLE user_xiuxian(user_id TEXT PRIMARY KEY,exp INTEGER)")
                conn.execute("INSERT INTO user_xiuxian VALUES('u',100)")
            with DatabaseUnitOfWork(db) as uow:
                apply_admin_stone_adjustment(uow)
                apply_admin_exp_adjustment(uow)
            repo = AdminExpAdjustmentSqlRepository(db)
            first = repo.adjust("e1", "admin", "u", 100, 25)
            duplicate = repo.adjust("e1", "admin", "u", 100, 25)
            stale = repo.adjust("e2", "admin", "u", 100, 25)
            self.assertEqual((first.status, duplicate.status, stale.status), ("adjusted", "duplicate", "state_changed"))
            with DatabaseUnitOfWork(db) as uow:
                self.assertEqual(
                    uow.query_one("SELECT COUNT(*) AS count FROM admin_exp_adjustment_operations")["count"],
                    1,
                )
                audit = uow.query_one(
                    "SELECT exp_delta,trace_id FROM economy_log WHERE user_id='u'"
                )
            self.assertEqual((audit["exp_delta"], audit["trace_id"]), (25, "e1"))

    def test_missing_startup_migration_fails_closed_without_request_ddl(self):
        with tempfile.TemporaryDirectory() as temp:
            db = Path(temp) / "game.db"
            with db_backend.transaction(db) as conn:
                conn.execute("CREATE TABLE user_xiuxian(user_id TEXT PRIMARY KEY,exp INTEGER)")
                conn.execute("INSERT INTO user_xiuxian VALUES('u',100)")

            result = AdminExpAdjustmentSqlRepository(db).adjust("e1", "admin", "u", 100, 25)
            self.assertEqual(result.status, "schema_missing")
            with db_backend.transaction(db) as conn:
                self.assertIsNone(
                    conn.execute(
                        "SELECT 1 FROM sqlite_master WHERE type='table' "
                        "AND name IN ('admin_exp_adjustment_operations','economy_log')"
                    ).fetchone()
                )
                self.assertEqual(conn.execute("SELECT exp FROM user_xiuxian WHERE user_id='u'").fetchone()[0], 100)

    def test_adjust_all_is_atomic_and_replayable(self):
        with tempfile.TemporaryDirectory() as temp:
            db = Path(temp) / "game.db"
            with db_backend.transaction(db) as conn:
                conn.execute("CREATE TABLE user_xiuxian(user_id TEXT PRIMARY KEY,exp INTEGER)")
                conn.executemany(
                    "INSERT INTO user_xiuxian VALUES(?,?)",
                    [("u1", 100), ("u2", 0), ("u3", None)],
                )
            with DatabaseUnitOfWork(db) as uow:
                apply_admin_exp_batch_adjustment(uow)

            repo = AdminExpAdjustmentSqlRepository(db)
            first = repo.adjust_all("all-1", "admin", -25)
            duplicate = repo.adjust_all("all-1", "admin", -25)
            conflict = repo.adjust_all("all-1", "admin", 50)
            with DatabaseUnitOfWork(db, read_only=True) as uow:
                values = [
                    row["exp"]
                    for row in uow.query_all(
                        "SELECT exp FROM user_xiuxian ORDER BY user_id"
                    )
                ]
                operations = uow.query_one(
                    "SELECT COUNT(*) AS count FROM admin_exp_batch_adjustment_operations"
                )["count"]

            self.assertEqual((first.status, first.affected_users, first.applied_delta), ("adjusted", 3, -75))
            self.assertEqual((duplicate.status, duplicate.affected_users, duplicate.applied_delta), ("duplicate", 3, -75))
            self.assertEqual(conflict.status, "operation_conflict")
            self.assertEqual(values, [75, -25, -25])
            self.assertEqual(operations, 1)

    def test_adjust_all_fails_closed_without_startup_migration(self):
        with tempfile.TemporaryDirectory() as temp:
            db = Path(temp) / "game.db"
            with db_backend.transaction(db) as conn:
                conn.execute("CREATE TABLE user_xiuxian(user_id TEXT PRIMARY KEY,exp INTEGER)")
                conn.execute("INSERT INTO user_xiuxian VALUES('u',100)")

            result = AdminExpAdjustmentSqlRepository(db).adjust_all("all-1", "admin", 25)
            with db_backend.transaction(db) as conn:
                table = conn.execute(
                    "SELECT 1 FROM sqlite_master WHERE type='table' "
                    "AND name='admin_exp_batch_adjustment_operations'"
                ).fetchone()
                exp = conn.execute("SELECT exp FROM user_xiuxian WHERE user_id='u'").fetchone()[0]

            self.assertEqual(result.status, "schema_missing")
            self.assertIsNone(table)
            self.assertEqual(exp, 100)

    def test_adjust_all_rolls_back_every_user_when_receipt_write_fails(self):
        with tempfile.TemporaryDirectory() as temp:
            db = Path(temp) / "game.db"
            with db_backend.transaction(db) as conn:
                conn.execute("CREATE TABLE user_xiuxian(user_id TEXT PRIMARY KEY,exp INTEGER)")
                conn.executemany(
                    "INSERT INTO user_xiuxian VALUES(?,?)", [("u1", 100), ("u2", 0)]
                )
            with DatabaseUnitOfWork(db) as uow:
                apply_admin_exp_batch_adjustment(uow)
            with db_backend.transaction(db) as conn:
                conn.execute(
                    "CREATE TRIGGER reject_admin_exp_batch_receipt "
                    "BEFORE INSERT ON admin_exp_batch_adjustment_operations "
                    "BEGIN SELECT RAISE(ABORT, 'receipt rejected'); END"
                )

            with self.assertRaises(sqlite3.IntegrityError):
                AdminExpAdjustmentSqlRepository(db).adjust_all("all-failed", "admin", -25)

            with DatabaseUnitOfWork(db, read_only=True) as uow:
                values = [
                    row["exp"]
                    for row in uow.query_all("SELECT exp FROM user_xiuxian ORDER BY user_id")
                ]
                receipts = uow.query_one(
                    "SELECT COUNT(*) AS count FROM admin_exp_batch_adjustment_operations"
                )["count"]

            self.assertEqual(values, [100, 0])
            self.assertEqual(receipts, 0)
