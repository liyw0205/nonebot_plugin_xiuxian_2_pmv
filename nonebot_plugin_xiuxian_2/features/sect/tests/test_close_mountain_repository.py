import tempfile
import unittest
from pathlib import Path
from ..close_mountain_repository import SectCloseMountainSqlRepository
from tests.test_db_backend import db_backend

class SectCloseMountainRepositoryTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.database = Path(self.temp.name) / 'sect.db'
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
        with db_backend.connection(self.database) as conn:
            self.assertEqual(tuple(conn.execute("SELECT sect_position FROM user_xiuxian WHERE user_id='owner'").fetchone()), (2,))
            self.assertEqual(tuple(conn.execute('SELECT closed,join_open,sect_owner FROM sects WHERE sect_id=1').fetchone()), (1,0,None))
