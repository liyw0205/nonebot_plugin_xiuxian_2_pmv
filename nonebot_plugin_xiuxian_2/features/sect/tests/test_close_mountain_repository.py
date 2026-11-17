import tempfile
import unittest
from pathlib import Path
from ..close_mountain_repository import SectCloseMountainSqlRepository
from tests.test_db_backend import db_backend
from .fixtures import migrate_sect

class SectCloseMountainRepositoryTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.database = Path(self.temp.name) / 'sect.db'
        migrate_sect(self.database)
        with db_backend.transaction(self.database) as conn:
            conn.execute('CREATE TABLE user_xiuxian(user_id TEXT PRIMARY KEY,sect_id INTEGER,sect_position INTEGER)')
            conn.execute("INSERT INTO user_xiuxian VALUES('owner',1,0)")
            conn.execute('CREATE TABLE sects(sect_id INTEGER PRIMARY KEY,sect_owner TEXT,sect_name TEXT,closed INTEGER,join_open INTEGER)')
            conn.execute("INSERT INTO sects VALUES(1,'owner','青云',0,1)")
        self.repo = SectCloseMountainSqlRepository(self.database)
    def tearDown(self): self.temp.cleanup()
    def test_close_replays_and_updates_owner_and_sect_atomically(self):
        first = self.repo.close('op','owner')
        duplicate = self.repo.close('op','owner')
        self.assertEqual((first['status'], duplicate['status']), ('closed','duplicate'))
        conflict = self.repo.close('op','other')
        self.assertEqual((conflict['status'], conflict['reason'], conflict['subject']), ('operation_conflict', 'operation_conflict', 'actor'))
        with db_backend.connection(self.database) as conn:
            self.assertEqual(tuple(conn.execute("SELECT sect_position FROM user_xiuxian WHERE user_id='owner'").fetchone()), (2,))
            self.assertEqual(tuple(conn.execute('SELECT closed,join_open,sect_owner FROM sects WHERE sect_id=1').fetchone()), (1,0,None))

    def test_member_rewrite_rolls_back_with_specific_reason(self):
        with db_backend.transaction(self.database) as conn:
            conn.execute("CREATE TRIGGER rewrite_member AFTER UPDATE OF sect_position ON user_xiuxian WHEN NEW.sect_position=2 BEGIN UPDATE user_xiuxian SET sect_position=3 WHERE user_id='owner'; END")
        result = self.repo.close('rewrite-member', 'owner')
        self.assertEqual((result['status'], result['reason'], result['subject']), ('state_changed', 'sect_close_mountain_member_write_rejected', 'actor'))
        with db_backend.connection(self.database) as conn:
            self.assertEqual(tuple(conn.execute("SELECT sect_position FROM user_xiuxian WHERE user_id='owner'").fetchone()), (0,))
            self.assertEqual(tuple(conn.execute('SELECT closed,join_open,sect_owner FROM sects WHERE sect_id=1').fetchone()), (0,1,'owner'))

    def test_sect_rewrite_rolls_back_with_specific_reason(self):
        with db_backend.transaction(self.database) as conn:
            conn.execute("CREATE TRIGGER rewrite_state AFTER UPDATE OF closed ON sects WHEN NEW.closed=1 BEGIN UPDATE sects SET closed=0,join_open=1,sect_owner='intruder' WHERE sect_id=1; END")
        result = self.repo.close('rewrite-state', 'owner')
        self.assertEqual((result['status'], result['reason'], result['subject']), ('state_changed', 'sect_close_mountain_state_write_rejected', 'sect'))
        with db_backend.connection(self.database) as conn:
            self.assertEqual(tuple(conn.execute('SELECT closed,join_open,sect_owner FROM sects WHERE sect_id=1').fetchone()), (0,1,'owner'))

    def test_receipt_rejection_rolls_back_and_recovers(self):
        for trigger, name in (
            ("CREATE TRIGGER ignore_receipt BEFORE INSERT ON sect_close_mountain_operations BEGIN SELECT RAISE(IGNORE); END", 'ignore_receipt'),
            ("CREATE TRIGGER rewrite_receipt AFTER INSERT ON sect_close_mountain_operations BEGIN UPDATE sect_close_mountain_operations SET actor_id='other' WHERE operation_id='recover'; END", 'rewrite_receipt'),
        ):
            with self.subTest(trigger=trigger):
                with db_backend.transaction(self.database) as conn:
                    conn.execute(trigger)
                failed = self.repo.close('recover', 'owner')
                self.assertEqual((failed['status'], failed['reason'], failed['subject']), ('state_changed', 'sect_close_mountain_receipt_write_rejected', 'recover'))
                with db_backend.connection(self.database) as conn:
                    self.assertEqual(tuple(conn.execute('SELECT closed,join_open,sect_owner FROM sects WHERE sect_id=1').fetchone()), (0,1,'owner'))
                with db_backend.transaction(self.database) as conn:
                    conn.execute(f'DROP TRIGGER {name}')
                self.assertEqual(self.repo.close('recover', 'owner')['status'], 'closed')
                with db_backend.transaction(self.database) as conn:
                    conn.execute("DELETE FROM sect_close_mountain_operations WHERE operation_id='recover'")
                    conn.execute("UPDATE user_xiuxian SET sect_position=0 WHERE user_id='owner'")
                    conn.execute("UPDATE sects SET closed=0,join_open=1,sect_owner='owner' WHERE sect_id=1")

    def test_abort_preserves_database_error(self):
        with db_backend.transaction(self.database) as conn:
            conn.execute("CREATE TRIGGER abort_state BEFORE UPDATE ON sects BEGIN SELECT RAISE(ABORT, 'close mountain boom'); END")
        with self.assertRaisesRegex(db_backend.Error, 'close mountain boom'):
            self.repo.close('abort', 'owner')
        with db_backend.connection(self.database) as conn:
            self.assertEqual(tuple(conn.execute('SELECT closed,join_open,sect_owner FROM sects WHERE sect_id=1').fetchone()), (0,1,'owner'))
