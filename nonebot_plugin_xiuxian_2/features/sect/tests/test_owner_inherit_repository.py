import tempfile
import unittest
from pathlib import Path
from ..owner_inherit_repository import SectOwnerInheritSqlRepository
from tests.test_db_backend import db_backend

class SectOwnerInheritRepositoryTests(unittest.TestCase):
    def test_inherit_replays_and_reopens_closed_sect(self):
        with tempfile.TemporaryDirectory() as temp:
            db = Path(temp) / 'sect.db'
            with db_backend.transaction(db) as c:
                c.execute('CREATE TABLE user_xiuxian(user_id TEXT PRIMARY KEY,sect_id INTEGER,sect_position INTEGER,user_name TEXT,sect_contribution INTEGER)')
                c.execute("INSERT INTO user_xiuxian VALUES('candidate',1,2,'候选人',10)")
                c.execute('CREATE TABLE sects(sect_id INTEGER PRIMARY KEY,sect_owner TEXT,sect_name TEXT,closed INTEGER,join_open INTEGER)')
                c.execute("INSERT INTO sects VALUES(1,NULL,'青云',1,0)")
            repo = SectOwnerInheritSqlRepository(db)
            first = repo.inherit('op','candidate',expected_sect_id=1,eligible_positions=(1,2))
            duplicate = repo.inherit('op','candidate',expected_sect_id=1,eligible_positions=(1,2))
            self.assertEqual((first['status'],duplicate['status']),('inherited','duplicate'))
            with db_backend.connection(db) as c:
                self.assertEqual(tuple(c.execute('SELECT sect_owner,closed,join_open FROM sects WHERE sect_id=1').fetchone()),('candidate',0,1))
