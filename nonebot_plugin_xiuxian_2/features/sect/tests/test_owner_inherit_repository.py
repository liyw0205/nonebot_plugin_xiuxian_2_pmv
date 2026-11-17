import tempfile
import unittest
from pathlib import Path
from ..owner_inherit_repository import SectOwnerInheritSqlRepository
from tests.test_db_backend import db_backend
from .fixtures import migrate_sect

class SectOwnerInheritRepositoryTests(unittest.TestCase):
    def _database(self, temp):
        db = Path(temp) / 'sect.db'
        migrate_sect(db)
        with db_backend.transaction(db) as c:
            c.execute('CREATE TABLE user_xiuxian(user_id TEXT PRIMARY KEY,sect_id INTEGER,sect_position INTEGER,user_name TEXT,sect_contribution INTEGER)')
            c.execute("INSERT INTO user_xiuxian VALUES('candidate',1,2,'候选人',10)")
            c.execute('CREATE TABLE sects(sect_id INTEGER PRIMARY KEY,sect_owner TEXT,sect_name TEXT,closed INTEGER,join_open INTEGER)')
            c.execute("INSERT INTO sects VALUES(1,NULL,'青云',1,0)")
        return db

    def test_inherit_replays_and_reopens_closed_sect(self):
        with tempfile.TemporaryDirectory() as temp:
            db = Path(temp) / 'sect.db'
            migrate_sect(db)
            with db_backend.transaction(db) as c:
                c.execute('CREATE TABLE user_xiuxian(user_id TEXT PRIMARY KEY,sect_id INTEGER,sect_position INTEGER,user_name TEXT,sect_contribution INTEGER)')
                c.execute("INSERT INTO user_xiuxian VALUES('candidate',1,2,'候选人',10)")
                c.execute('CREATE TABLE sects(sect_id INTEGER PRIMARY KEY,sect_owner TEXT,sect_name TEXT,closed INTEGER,join_open INTEGER)')
                c.execute("INSERT INTO sects VALUES(1,NULL,'青云',1,0)")
            repo = SectOwnerInheritSqlRepository(db)
            first = repo.inherit('op','candidate',expected_sect_id=1,eligible_positions=(1,2))
            duplicate = repo.inherit('op','candidate',expected_sect_id=1,eligible_positions=(1,2))
            self.assertEqual((first['status'],duplicate['status']),('inherited','duplicate'))
            conflict = repo.inherit('op','other',expected_sect_id=1,eligible_positions=(1,2))
            self.assertEqual((conflict['status'], conflict['reason'], conflict['subject']), ('operation_conflict', 'operation_conflict', 'actor'))
            with db_backend.connection(db) as c:
                self.assertEqual(tuple(c.execute('SELECT sect_owner,closed,join_open FROM sects WHERE sect_id=1').fetchone()),('candidate',0,1))

    def test_member_rewrite_rolls_back_with_specific_reason(self):
        with tempfile.TemporaryDirectory() as temp:
            db = self._database(temp)
            with db_backend.transaction(db) as c:
                c.execute("CREATE TRIGGER rewrite_member AFTER UPDATE OF sect_position ON user_xiuxian WHEN NEW.sect_position=0 BEGIN UPDATE user_xiuxian SET sect_position=2 WHERE user_id='candidate'; END")
            result = SectOwnerInheritSqlRepository(db).inherit('rewrite', 'candidate', expected_sect_id=1, eligible_positions=(1,2))
            self.assertEqual((result['status'], result['reason'], result['subject']), ('state_changed', 'sect_owner_inherit_member_write_rejected', 'actor'))
            with db_backend.connection(db) as c:
                self.assertEqual(tuple(c.execute("SELECT sect_position FROM user_xiuxian WHERE user_id='candidate'").fetchone()), (2,))
                self.assertEqual(tuple(c.execute('SELECT sect_owner,closed,join_open FROM sects WHERE sect_id=1').fetchone()), (None,1,0))

    def test_sect_rewrite_rolls_back_with_specific_reason(self):
        with tempfile.TemporaryDirectory() as temp:
            db = self._database(temp)
            with db_backend.transaction(db) as c:
                c.execute("CREATE TRIGGER rewrite_state AFTER UPDATE OF sect_owner ON sects WHEN NEW.sect_owner='candidate' BEGIN UPDATE sects SET sect_owner=NULL,closed=1,join_open=0 WHERE sect_id=1; END")
            result = SectOwnerInheritSqlRepository(db).inherit('rewrite', 'candidate', expected_sect_id=1, eligible_positions=(1,2))
            self.assertEqual((result['status'], result['reason'], result['subject']), ('state_changed', 'sect_owner_inherit_state_write_rejected', 'sect'))
            with db_backend.connection(db) as c:
                self.assertEqual(tuple(c.execute("SELECT sect_position FROM user_xiuxian WHERE user_id='candidate'").fetchone()), (2,))
                self.assertEqual(tuple(c.execute('SELECT sect_owner,closed,join_open FROM sects WHERE sect_id=1').fetchone()), (None,1,0))

    def test_receipt_rejection_rolls_back_and_recovers(self):
        for trigger, name in (
            ("CREATE TRIGGER ignore_receipt BEFORE INSERT ON sect_owner_inherit_operations BEGIN SELECT RAISE(IGNORE); END", 'ignore_receipt'),
            ("CREATE TRIGGER rewrite_receipt AFTER INSERT ON sect_owner_inherit_operations BEGIN UPDATE sect_owner_inherit_operations SET actor_id='other' WHERE operation_id='recover'; END", 'rewrite_receipt'),
        ):
            with self.subTest(trigger=trigger), tempfile.TemporaryDirectory() as temp:
                db = self._database(temp)
                with db_backend.transaction(db) as c:
                    c.execute(trigger)
                repo = SectOwnerInheritSqlRepository(db)
                failed = repo.inherit('recover', 'candidate', expected_sect_id=1, eligible_positions=(1,2))
                self.assertEqual((failed['status'], failed['reason'], failed['subject']), ('state_changed', 'sect_owner_inherit_receipt_write_rejected', 'recover'))
                with db_backend.connection(db) as c:
                    self.assertEqual(tuple(c.execute('SELECT sect_owner,closed,join_open FROM sects WHERE sect_id=1').fetchone()), (None,1,0))
                with db_backend.transaction(db) as c:
                    c.execute(f'DROP TRIGGER {name}')
                self.assertEqual(repo.inherit('recover', 'candidate', expected_sect_id=1, eligible_positions=(1,2))['status'], 'inherited')

    def test_abort_preserves_database_error(self):
        with tempfile.TemporaryDirectory() as temp:
            db = self._database(temp)
            with db_backend.transaction(db) as c:
                c.execute("CREATE TRIGGER abort_state BEFORE UPDATE ON sects BEGIN SELECT RAISE(ABORT, 'owner inherit boom'); END")
            with self.assertRaisesRegex(db_backend.Error, 'owner inherit boom'):
                SectOwnerInheritSqlRepository(db).inherit('abort', 'candidate', expected_sect_id=1, eligible_positions=(1,2))
