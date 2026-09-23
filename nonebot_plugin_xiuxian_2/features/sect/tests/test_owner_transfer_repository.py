import tempfile
import unittest
from pathlib import Path
from ..owner_transfer_repository import SectOwnerTransferSqlRepository
from tests.test_db_backend import db_backend

class SectOwnerTransferRepositoryTests(unittest.TestCase):
    def test_transfer_replays_and_swaps_owner_positions(self):
        with tempfile.TemporaryDirectory() as temp:
            db=Path(temp)/'sect.db'
            with db_backend.transaction(db) as c:
                c.execute('CREATE TABLE user_xiuxian(user_id TEXT PRIMARY KEY,sect_id INTEGER,sect_position INTEGER,user_name TEXT)')
                c.execute("INSERT INTO user_xiuxian VALUES('owner',1,0,'宗主')")
                c.execute("INSERT INTO user_xiuxian VALUES('target',1,2,'长老')")
                c.execute('CREATE TABLE sects(sect_id INTEGER PRIMARY KEY,sect_owner TEXT,sect_name TEXT)')
                c.execute("INSERT INTO sects VALUES(1,'owner','青云')")
            repo=SectOwnerTransferSqlRepository(db)
            self.assertEqual(repo.transfer('op','owner','target')['status'],'transferred')
            self.assertEqual(repo.transfer('op','owner','target')['status'],'duplicate')
