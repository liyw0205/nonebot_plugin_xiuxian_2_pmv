import tempfile
import unittest
from pathlib import Path
from ..owner_transfer_repository import SectOwnerTransferSqlRepository
from tests.test_db_backend import db_backend
from .fixtures import migrate_sect

class SectOwnerTransferRepositoryTests(unittest.TestCase):
    def _database(self, temp):
        db = Path(temp) / 'sect.db'
        migrate_sect(db)
        with db_backend.transaction(db) as c:
            c.execute("CREATE TABLE user_xiuxian(user_id TEXT PRIMARY KEY,sect_id INTEGER,sect_position INTEGER,user_name TEXT)")
            c.execute("INSERT INTO user_xiuxian VALUES('owner',1,0,'宗主')")
            c.execute("INSERT INTO user_xiuxian VALUES('target',1,2,'长老')")
            c.execute("CREATE TABLE sects(sect_id INTEGER PRIMARY KEY,sect_owner TEXT,sect_name TEXT)")
            c.execute("INSERT INTO sects VALUES(1,'owner','青云')")
        return db

    def _state(self, db):
        with db_backend.connection(db) as c:
            return (
                tuple(c.execute("SELECT sect_owner FROM sects WHERE sect_id=1").fetchone()),
                [tuple(row) for row in c.execute("SELECT user_id,sect_position FROM user_xiuxian ORDER BY user_id").fetchall()],
                c.execute("SELECT COUNT(*) FROM sect_operations").fetchone()[0],
            )

    def test_transfer_replays_and_swaps_owner_positions(self):
        with tempfile.TemporaryDirectory() as temp:
            db=self._database(temp)
            repo=SectOwnerTransferSqlRepository(db)
            first = repo.transfer('op','owner','target')
            duplicate = repo.transfer('op','owner','target')
            self.assertEqual(first['status'],'transferred')
            self.assertEqual(duplicate['status'],'duplicate')
            self.assertEqual(self._state(db), (('target',), [('owner', 1), ('target', 0)], 1))

    def test_changed_payload_is_operation_conflict(self):
        with tempfile.TemporaryDirectory() as temp:
            db = self._database(temp)
            repo = SectOwnerTransferSqlRepository(db)
            self.assertEqual(repo.transfer('op','owner','target')['status'], 'transferred')
            result = repo.transfer('op','owner','missing')
            self.assertEqual((result['status'], result['reason'], result['subject']), ('operation_conflict', 'operation_conflict', 'actor'))

    def test_configured_owner_position_moves_former_owner_to_next_rank(self):
        with tempfile.TemporaryDirectory() as temp:
            db = self._database(temp)
            with db_backend.transaction(db) as c:
                c.execute("UPDATE user_xiuxian SET sect_position=3 WHERE user_id='owner'")
                c.execute("UPDATE user_xiuxian SET sect_position=5 WHERE user_id='target'")
                c.execute("UPDATE sects SET sect_owner='owner' WHERE sect_id=1")
            result = SectOwnerTransferSqlRepository(db).transfer('configured', 'owner', 'target', owner_position=3, former_owner_position=4)
            self.assertEqual(result['status'], 'transferred')
            self.assertEqual(self._state(db), (('target',), [('owner', 4), ('target', 3)], 1))

    def test_member_rewrite_rolls_back_with_specific_reason(self):
        with tempfile.TemporaryDirectory() as temp:
            db = self._database(temp)
            with db_backend.transaction(db) as c:
                c.execute("CREATE TRIGGER rewrite_target AFTER UPDATE OF sect_position ON user_xiuxian WHEN NEW.user_id='target' AND NEW.sect_position=0 BEGIN UPDATE user_xiuxian SET sect_position=4 WHERE user_id='target'; END")
            result = SectOwnerTransferSqlRepository(db).transfer('rewrite', 'owner', 'target')
            self.assertEqual((result['status'], result['reason'], result['subject']), ('state_changed', 'sect_owner_transfer_target_write_rejected', 'target'))
            self.assertEqual(self._state(db), (('owner',), [('owner', 0), ('target', 2)], 0))

    def test_owner_rewrite_rolls_back_with_specific_reason(self):
        with tempfile.TemporaryDirectory() as temp:
            db = self._database(temp)
            with db_backend.transaction(db) as c:
                c.execute("CREATE TRIGGER rewrite_owner AFTER UPDATE OF sect_owner ON sects WHEN NEW.sect_owner='target' BEGIN UPDATE sects SET sect_owner='intruder' WHERE sect_id=1; END")
            result = SectOwnerTransferSqlRepository(db).transfer('rewrite', 'owner', 'target')
            self.assertEqual((result['status'], result['reason'], result['subject']), ('state_changed', 'sect_owner_transfer_sect_write_rejected', 'sect'))
            self.assertEqual(self._state(db), (('owner',), [('owner', 0), ('target', 2)], 0))

    def test_receipt_ignore_and_rewrite_roll_back_and_recover(self):
        for trigger, reason in (
            ("CREATE TRIGGER ignore_receipt BEFORE INSERT ON sect_operations BEGIN SELECT RAISE(IGNORE); END", 'sect_owner_transfer_receipt_write_rejected'),
            ("CREATE TRIGGER rewrite_receipt AFTER INSERT ON sect_operations BEGIN UPDATE sect_operations SET operation_type='other' WHERE operation_id='recover'; END", 'sect_owner_transfer_receipt_write_rejected'),
        ):
            with self.subTest(trigger=trigger), tempfile.TemporaryDirectory() as temp:
                db = self._database(temp)
                with db_backend.transaction(db) as c:
                    c.execute(trigger)
                repo = SectOwnerTransferSqlRepository(db)
                failed = repo.transfer('recover', 'owner', 'target')
                self.assertEqual((failed['status'], failed['reason'], failed['subject']), ('state_changed', reason, 'recover'))
                self.assertEqual(self._state(db), (('owner',), [('owner', 0), ('target', 2)], 0))
                with db_backend.transaction(db) as c:
                    c.execute('DROP TRIGGER ' + ('ignore_receipt' if 'ignore_receipt' in trigger else 'rewrite_receipt'))
                recovered = repo.transfer('recover', 'owner', 'target')
                self.assertEqual(recovered['status'], 'transferred')
                self.assertEqual(self._state(db), (('target',), [('owner', 1), ('target', 0)], 1))

    def test_abort_preserves_database_error_and_rolls_back(self):
        with tempfile.TemporaryDirectory() as temp:
            db = self._database(temp)
            with db_backend.transaction(db) as c:
                c.execute("CREATE TRIGGER abort_owner BEFORE UPDATE ON sects BEGIN SELECT RAISE(ABORT, 'owner transfer boom'); END")
            with self.assertRaisesRegex(db_backend.Error, 'owner transfer boom'):
                SectOwnerTransferSqlRepository(db).transfer('abort', 'owner', 'target')
            self.assertEqual(self._state(db), (('owner',), [('owner', 0), ('target', 2)], 0))
