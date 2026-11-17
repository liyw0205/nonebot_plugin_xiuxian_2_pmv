import sqlite3
import tempfile
import unittest
from pathlib import Path

from ..repository import SectRenameSqlRepository
from tests.test_db_backend import db_backend


class SectRenameRepositoryTests(unittest.TestCase):
    def setUp(self):
        self.t = tempfile.TemporaryDirectory()
        self.db = Path(self.t.name) / "s.db"
        with db_backend.transaction(self.db) as connection:
            connection.execute(
                "CREATE TABLE user_xiuxian(user_id TEXT PRIMARY KEY,sect_id INTEGER,sect_position INTEGER)"
            )
            connection.execute("INSERT INTO user_xiuxian VALUES('owner',1,0)")
            connection.execute(
                "CREATE TABLE sects(sect_id INTEGER PRIMARY KEY,sect_name TEXT,sect_owner TEXT,sect_used_stone INTEGER)"
            )
            connection.execute("INSERT INTO sects VALUES(1,'旧宗','owner',1000)")
            connection.execute(
                "CREATE TABLE back(user_id TEXT,goods_id INTEGER,goods_num INTEGER,bind_num INTEGER)"
            )
            connection.execute("INSERT INTO back VALUES('owner',1999,2,1)")
            connection.execute(
                "CREATE TABLE sect_rename_operations(operation_id TEXT PRIMARY KEY,payload TEXT,previous_name TEXT,new_name TEXT)"
            )
        self.repository = SectRenameSqlRepository(self.db)

    def tearDown(self):
        self.t.cleanup()

    def snapshot(self):
        with db_backend.connection(self.db) as connection:
            return (
                tuple(connection.execute(
                    "SELECT sect_name,sect_used_stone FROM sects WHERE sect_id=1"
                ).fetchone()),
                tuple(connection.execute(
                    "SELECT goods_num,bind_num FROM back WHERE user_id='owner' AND goods_id=1999"
                ).fetchone()),
                connection.execute("SELECT COUNT(*) FROM sect_rename_operations").fetchone()[0],
            )

    def test_success_duplicate_and_conflict(self):
        first = self.repository.rename("x", "owner", 1, "新宗", 300, 1999)
        duplicate = self.repository.rename("x", "owner", 1, "别名", 999, 9999)
        conflict = self.repository.rename("y", "owner", 1, "新宗", 300, 1999)
        self.assertEqual(
            (first["status"], duplicate["status"], conflict["status"]),
            ("renamed", "duplicate", "name_exists"),
        )
        self.assertEqual(self.snapshot(), (("新宗", 700), (1, 0), 1))

    def test_ignore_at_each_write_returns_reason_and_recovers(self):
        cases = (
            ("sects", "UPDATE", "sect_rename_state_write_rejected", "sect"),
            ("back", "UPDATE", "sect_rename_card_write_rejected", "actor"),
            ("sect_rename_operations", "INSERT", "sect_rename_receipt_write_rejected", "actor"),
        )
        for table, verb, reason, subject in cases:
            with self.subTest(table=table):
                self.t.cleanup()
                self.setUp()
                with db_backend.transaction(self.db) as connection:
                    connection.execute(
                        f"CREATE TRIGGER refuse_rename BEFORE {verb} ON {table} "
                        "BEGIN SELECT RAISE(IGNORE); END"
                    )
                before = self.snapshot()
                failed = self.repository.rename("recover-" + table, "owner", 1, "恢复宗", 300, 1999)
                self.assertEqual(
                    (failed["status"], failed["reason"], failed["subject"]),
                    ("write_rejected", reason, subject),
                )
                self.assertEqual(self.snapshot(), before)
                with db_backend.transaction(self.db) as connection:
                    connection.execute("DROP TRIGGER refuse_rename")
                recovered = self.repository.rename("recover-" + table, "owner", 1, "恢复宗", 300, 1999)
                self.assertEqual(recovered["status"], "renamed")
                self.assertEqual(self.snapshot(), (("恢复宗", 700), (1, 0), before[2] + 1))

    def test_trigger_rewrite_is_rejected_and_rolled_back(self):
        with db_backend.transaction(self.db) as connection:
            connection.execute(
                "CREATE TRIGGER rewrite_rename AFTER UPDATE OF sect_name ON sects "
                "BEGIN UPDATE sects SET sect_name='篡改宗' WHERE sect_id=NEW.sect_id; END"
            )
        before = self.snapshot()
        result = self.repository.rename("rewrite", "owner", 1, "新宗", 300, 1999)
        self.assertEqual(
            (result["status"], result["reason"]),
            ("write_rejected", "sect_rename_state_write_rejected"),
        )
        self.assertEqual(self.snapshot(), before)

    def test_abort_is_transparent_and_rolls_back(self):
        with db_backend.transaction(self.db) as connection:
            connection.execute(
                "CREATE TRIGGER abort_rename BEFORE INSERT ON sect_rename_operations "
                "BEGIN SELECT RAISE(ABORT,'rename receipt failed'); END"
            )
        before = self.snapshot()
        with self.assertRaises(sqlite3.IntegrityError):
            self.repository.rename("abort", "owner", 1, "新宗", 300, 1999)
        self.assertEqual(self.snapshot(), before)


if __name__ == "__main__":
    unittest.main()
