import tempfile
import unittest
from pathlib import Path

from ....infrastructure.database import DatabaseUnitOfWork
from ..application import SectApplication
from ..manual_disband_repository import SectManualDisbandSqlRepository
from ..migrations import apply_sect_manual_disband
from tests.test_db_backend import db_backend


class SectManualDisbandRepositoryTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.database = Path(self.temp.name) / "sect.sqlite3"
        with db_backend.transaction(self.database) as conn:
            conn.execute("CREATE TABLE sects(sect_id INTEGER PRIMARY KEY,sect_name TEXT,sect_owner TEXT)")
            conn.execute("CREATE TABLE user_xiuxian(user_id TEXT PRIMARY KEY,sect_id INTEGER,sect_position INTEGER,sect_contribution INTEGER)")
            conn.execute("INSERT INTO sects VALUES(1,'青云','owner')")
            conn.execute("INSERT INTO user_xiuxian VALUES('owner',1,0,100)")
            conn.execute("INSERT INTO user_xiuxian VALUES('elder',1,2,80)")
            conn.execute("INSERT INTO user_xiuxian VALUES('outsider',NULL,NULL,30)")
        with DatabaseUnitOfWork(self.database) as uow:
            apply_sect_manual_disband(uow)
        self.repository = SectManualDisbandSqlRepository(self.database)

    def tearDown(self):
        self.temp.cleanup()

    def row(self, sql, params=()):
        with db_backend.connection(self.database) as conn:
            return conn.execute(sql, params).fetchone()

    def test_disband_unbinds_members_and_replays_receipt(self):
        first = self.repository.disband("op", "owner", expected_sect_id=1)
        duplicate = self.repository.disband("op", "different-actor", expected_sect_id=99)

        self.assertEqual(first, {
            "status": "disbanded", "actor_id": "owner", "sect_id": 1,
            "sect_name": "青云", "member_count": 2,
        })
        self.assertEqual(duplicate["status"], "duplicate")
        self.assertEqual((duplicate["sect_id"], duplicate["sect_name"], duplicate["member_count"]), (1, "青云", 2))
        self.assertIsNone(self.row("SELECT sect_id FROM sects WHERE sect_id=1"))
        self.assertEqual(self.row("SELECT COUNT(*) FROM user_xiuxian WHERE sect_id=1")[0], 0)
        self.assertEqual(tuple(self.row("SELECT sect_id,sect_position,sect_contribution FROM user_xiuxian WHERE user_id='elder'")), (None, None, 0))
        self.assertEqual(tuple(self.row("SELECT sect_id,sect_position,sect_contribution FROM user_xiuxian WHERE user_id='outsider'")), (None, None, 30))

    def test_rechecks_owner_and_expected_sect(self):
        self.assertEqual(self.repository.disband("stale-sect", "owner", expected_sect_id=2)["status"], "sect_changed")
        self.assertEqual(self.repository.disband("not-owner", "elder", expected_sect_id=1)["status"], "not_owner")
        self.assertEqual(self.row("SELECT COUNT(*) FROM sects WHERE sect_id=1")[0], 1)
        self.assertEqual(self.row("SELECT COUNT(*) FROM user_xiuxian WHERE sect_id=1")[0], 2)

    def test_receipt_failure_rolls_back_member_updates_and_delete(self):
        with db_backend.transaction(self.database) as conn:
            conn.execute(
                "CREATE TRIGGER fail_manual_disband BEFORE INSERT ON sect_disband_operations "
                "BEGIN SELECT RAISE(ABORT,'operation failed'); END"
            )
        with self.assertRaises(db_backend.IntegrityError):
            self.repository.disband("fail", "owner", expected_sect_id=1)
        self.assertEqual(self.row("SELECT COUNT(*) FROM sects WHERE sect_id=1")[0], 1)
        self.assertEqual(self.row("SELECT COUNT(*) FROM user_xiuxian WHERE sect_id=1")[0], 2)
        self.assertEqual(tuple(self.row("SELECT sect_id,sect_position,sect_contribution FROM user_xiuxian WHERE user_id='owner'")), (1, 0, 100))

    def test_missing_schema_is_rejected_without_request_ddl(self):
        other = Path(self.temp.name) / "unmigrated.sqlite3"
        with db_backend.transaction(other) as conn:
            conn.execute("CREATE TABLE sects(sect_id INTEGER PRIMARY KEY,sect_name TEXT,sect_owner TEXT)")
            conn.execute("CREATE TABLE user_xiuxian(user_id TEXT PRIMARY KEY,sect_id INTEGER,sect_position INTEGER,sect_contribution INTEGER)")
        result = SectManualDisbandSqlRepository(other).disband("op", "owner")
        self.assertEqual(result["status"], "schema_missing")
        with db_backend.connection(other) as conn:
            self.assertIsNone(conn.execute("SELECT 1 FROM sqlite_master WHERE type='table' AND name='sect_disband_operations'").fetchone())

    def test_startup_migration_preserves_existing_receipts(self):
        with db_backend.transaction(self.database) as conn:
            conn.execute("INSERT INTO sect_disband_operations(operation_id,actor_id,sect_id,sect_name,member_count) VALUES('old','owner',1,'旧名',2)")
        with DatabaseUnitOfWork(self.database) as uow:
            apply_sect_manual_disband(uow)
        old = self.row("SELECT sect_name,member_count FROM sect_disband_operations WHERE operation_id='old'")
        self.assertEqual(tuple(old), ("旧名", 2))

    def test_application_reports_success_as_applied(self):
        outcome = SectApplication(self.database).disband("app", "owner", expected_sect_id=1)
        self.assertTrue(outcome.applied)
        self.assertEqual(outcome.status, "disbanded")
