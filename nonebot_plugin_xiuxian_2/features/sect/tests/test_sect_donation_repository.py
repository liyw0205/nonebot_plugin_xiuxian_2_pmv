import sqlite3
import tempfile
import unittest
from pathlib import Path

from ..repository import SectRenameSqlRepository
from tests.test_db_backend import db_backend


class SectDonationRepositoryTests(unittest.TestCase):
    def setUp(self):
        self.t = tempfile.TemporaryDirectory()
        self.db = Path(self.t.name) / "s.db"
        with db_backend.transaction(self.db) as connection:
            connection.execute(
                "CREATE TABLE user_xiuxian(user_id TEXT PRIMARY KEY,sect_id INTEGER,"
                "stone INTEGER,sect_contribution INTEGER)"
            )
            connection.execute("INSERT INTO user_xiuxian VALUES('u',1,100,0)")
            connection.execute(
                "CREATE TABLE sects(sect_id INTEGER PRIMARY KEY,sect_used_stone INTEGER,"
                "sect_scale INTEGER,sect_materials INTEGER)"
            )
            connection.execute("INSERT INTO sects VALUES(1,0,0,0)")
            connection.execute(
                "CREATE TABLE sect_donation_operations(operation_id TEXT PRIMARY KEY,user_id TEXT,"
                "sect_id INTEGER,stone INTEGER,materials INTEGER)"
            )
        self.repository = SectRenameSqlRepository(self.db)

    def tearDown(self):
        self.t.cleanup()

    def snapshot(self):
        with db_backend.connection(self.db) as connection:
            return (
                tuple(connection.execute(
                    "SELECT stone,sect_contribution FROM user_xiuxian WHERE user_id='u'"
                ).fetchone()),
                tuple(connection.execute(
                    "SELECT sect_used_stone,sect_scale,sect_materials FROM sects WHERE sect_id=1"
                ).fetchone()),
                connection.execute("SELECT COUNT(*) FROM sect_donation_operations").fetchone()[0],
            )

    def test_success_duplicate_and_insufficient(self):
        first = self.repository.donate("x", "u", 1, 30, 3)
        duplicate = self.repository.donate("x", "u", 1, 99, 9)
        insufficient = self.repository.donate("y", "u", 1, 100, 1)
        self.assertEqual(
            (first["status"], duplicate["status"], insufficient["status"]),
            ("donated", "operation_conflict", "stone_insufficient"),
        )

    def test_ignore_at_each_write_returns_reason_and_recovers(self):
        cases = (
            ("user_xiuxian", "UPDATE", "sect_donation_member_write_rejected", "actor"),
            ("sects", "UPDATE", "sect_donation_assets_write_rejected", "sect"),
            ("sect_donation_operations", "INSERT", "sect_donation_receipt_write_rejected", "actor"),
        )
        for table, verb, reason, subject in cases:
            with self.subTest(table=table):
                with db_backend.transaction(self.db) as connection:
                    connection.execute(
                        f"CREATE TRIGGER refuse_donation BEFORE {verb} ON {table} "
                        "BEGIN SELECT RAISE(IGNORE); END"
                    )
                before = self.snapshot()
                failed = self.repository.donate("recover-" + table, "u", 1, 30, 3)
                self.assertEqual(
                    (failed["status"], failed["reason"], failed["subject"]),
                    ("write_rejected", reason, subject),
                )
                self.assertEqual(self.snapshot(), before)
                with db_backend.transaction(self.db) as connection:
                    connection.execute("DROP TRIGGER refuse_donation")
                recovered = self.repository.donate("recover-" + table, "u", 1, 30, 3)
                self.assertEqual(recovered["status"], "donated")
                self.assertEqual(self.snapshot()[2], before[2] + 1)

    def test_trigger_rewrite_is_rejected_and_rolled_back(self):
        with db_backend.transaction(self.db) as connection:
            connection.execute(
                "CREATE TRIGGER rewrite_donation AFTER UPDATE OF sect_contribution ON user_xiuxian "
                "BEGIN UPDATE user_xiuxian SET sect_contribution=sect_contribution+1 "
                "WHERE user_id=NEW.user_id; END"
            )
        before = self.snapshot()
        result = self.repository.donate("rewrite", "u", 1, 30, 3)
        self.assertEqual(
            (result["status"], result["reason"]),
            ("write_rejected", "sect_donation_member_write_rejected"),
        )
        self.assertEqual(self.snapshot(), before)

    def test_abort_is_transparent_and_rolls_back(self):
        with db_backend.transaction(self.db) as connection:
            connection.execute(
                "CREATE TRIGGER abort_donation BEFORE INSERT ON sect_donation_operations "
                "BEGIN SELECT RAISE(ABORT,'donation receipt failed'); END"
            )
        before = self.snapshot()
        with self.assertRaises(sqlite3.IntegrityError):
            self.repository.donate("abort", "u", 1, 30, 3)
        self.assertEqual(self.snapshot(), before)


if __name__ == "__main__":
    unittest.main()
