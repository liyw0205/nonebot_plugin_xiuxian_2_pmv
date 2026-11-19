import tempfile
import unittest
from pathlib import Path

from ..repository import SectRenameSqlRepository
from tests.test_db_backend import db_backend


class SectMemberRemovalRepositoryTests(unittest.TestCase):
    def setUp(self):
        self.tempdir = tempfile.TemporaryDirectory()
        self.database = Path(self.tempdir.name) / "member-removal.db"
        with db_backend.transaction(self.database) as conn:
            conn.execute("CREATE TABLE sects(sect_id INTEGER PRIMARY KEY,sect_name TEXT)")
            conn.execute("INSERT INTO sects VALUES(1,'测试宗')")
            conn.execute("CREATE TABLE user_xiuxian(user_id TEXT PRIMARY KEY,sect_id INTEGER,sect_position INTEGER,user_name TEXT,sect_contribution INTEGER)")
            conn.executemany("INSERT INTO user_xiuxian VALUES(?,?,?,?,?)", [("owner", 1, 0, "宗主", 0), ("member", 1, 5, "成员", 88)])
            conn.execute("CREATE TABLE sect_member_removal_operations(operation_id TEXT PRIMARY KEY,operation_type TEXT,actor_id TEXT,target_id TEXT,sect_id INTEGER,sect_name TEXT,actor_name TEXT,target_name TEXT,actor_position INTEGER,target_position INTEGER)")
        self.repository = SectRenameSqlRepository(self.database)

    def tearDown(self):
        self.tempdir.cleanup()

    def _member(self):
        with db_backend.connection(self.database) as conn:
            return tuple(conn.execute("SELECT sect_id,sect_position,sect_contribution FROM user_xiuxian WHERE user_id='member'").fetchone())

    def _receipts(self):
        with db_backend.connection(self.database) as conn:
            return int(conn.execute("SELECT COUNT(*) FROM sect_member_removal_operations").fetchone()[0])

    def test_kick_receipt_ignore_rolls_back_and_recovers(self):
        with db_backend.transaction(self.database) as conn:
            conn.execute("CREATE TRIGGER removal_receipt_ignore BEFORE INSERT ON sect_member_removal_operations BEGIN SELECT RAISE(IGNORE); END")
        failed = self.repository.kick("kick-recover", "owner", "member", manager_max_position=2)
        self.assertEqual((failed["status"], failed["reason"], failed["subject"]), ("state_changed", "sect_kick_receipt_write_rejected", "kick-recover"))
        self.assertEqual((self._member(), self._receipts()), ((1, 5, 88), 0))
        with db_backend.transaction(self.database) as conn:
            conn.execute("DROP TRIGGER removal_receipt_ignore")
        recovered = self.repository.kick("kick-recover", "owner", "member", manager_max_position=2)
        self.assertEqual(recovered["status"], "kicked")
        self.assertEqual((self._member(), self._receipts()), ((None, None, 0), 1))

    def test_leave_receipt_ignore_rolls_back_and_recovers(self):
        with db_backend.transaction(self.database) as conn:
            conn.execute("CREATE TRIGGER removal_receipt_ignore BEFORE INSERT ON sect_member_removal_operations BEGIN SELECT RAISE(IGNORE); END")
        failed = self.repository.leave("leave-recover", "member", owner_position=0)
        self.assertEqual((failed["status"], failed["reason"], failed["subject"]), ("state_changed", "sect_leave_receipt_write_rejected", "leave-recover"))
        self.assertEqual((self._member(), self._receipts()), ((1, 5, 88), 0))
        with db_backend.transaction(self.database) as conn:
            conn.execute("DROP TRIGGER removal_receipt_ignore")
        recovered = self.repository.leave("leave-recover", "member", owner_position=0)
        self.assertEqual(recovered["status"], "left")
        self.assertEqual((self._member(), self._receipts()), ((None, None, 0), 1))

    def test_member_trigger_rewrite_is_rejected_without_receipt(self):
        with db_backend.transaction(self.database) as conn:
            conn.execute("CREATE TRIGGER removal_member_rewrite AFTER UPDATE OF sect_id ON user_xiuxian WHEN NEW.user_id='member' BEGIN UPDATE user_xiuxian SET sect_contribution=7 WHERE user_id='member'; END")
        result = self.repository.kick("kick-rewrite", "owner", "member", manager_max_position=2)
        self.assertEqual((result["status"], result["reason"], result["subject"]), ("state_changed", "sect_kick_member_write_rejected", "member"))
        self.assertEqual((self._member(), self._receipts()), ((1, 5, 88), 0))

    def test_operation_payload_conflict_is_not_duplicate(self):
        first = self.repository.kick("kick-conflict", "owner", "member", manager_max_position=2)
        self.assertEqual(first["status"], "kicked")
        result = self.repository.leave("kick-conflict", "member", owner_position=0)
        self.assertEqual((result["status"], result["reason"], result["subject"]), ("operation_conflict", "operation_conflict", "actor"))

    def test_leave_receipt_abort_preserves_exception_and_rolls_back(self):
        with db_backend.transaction(self.database) as conn:
            conn.execute("CREATE TRIGGER removal_receipt_abort BEFORE INSERT ON sect_member_removal_operations BEGIN SELECT RAISE(ABORT, 'removal receipt boom'); END")
        with self.assertRaisesRegex(db_backend.Error, "removal receipt boom"):
            self.repository.leave("leave-abort", "member", owner_position=0)
        self.assertEqual((self._member(), self._receipts()), ((1, 5, 88), 0))


if __name__ == "__main__":
    unittest.main()
