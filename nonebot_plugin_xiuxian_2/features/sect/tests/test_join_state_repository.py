import tempfile
import unittest
from pathlib import Path
from ..join_state_repository import SectJoinStateSqlRepository
from tests.test_db_backend import db_backend

class SectJoinStateRepositoryTests(unittest.TestCase):
    def test_open_close_join_replay(self):
        with tempfile.TemporaryDirectory() as temp:
            db = Path(temp) / 'sect.db'
            with db_backend.transaction(db) as c:
                c.execute('CREATE TABLE user_xiuxian(user_id TEXT PRIMARY KEY,sect_id INTEGER,sect_position INTEGER)')
                c.execute("INSERT INTO user_xiuxian VALUES('owner',1,0)")
                c.execute('CREATE TABLE sects(sect_id INTEGER PRIMARY KEY,sect_owner TEXT,sect_name TEXT,join_open INTEGER,closed INTEGER)')
                c.execute("INSERT INTO sects VALUES(1,'owner','青云',0,0)")
            repo = SectJoinStateSqlRepository(db)
            opened = repo.open('open','owner',expected_sect_id=1)
            closed = repo.close('close','owner',expected_sect_id=1)
            self.assertEqual((opened['status'],closed['status']),('opened','closed'))
            self.assertEqual(repo.close('close','owner')['status'],'duplicate')
