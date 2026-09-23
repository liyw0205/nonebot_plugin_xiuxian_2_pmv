import tempfile
import unittest
from pathlib import Path
from ..fairyland_repository import SectFairylandSqlRepository
from tests.test_db_backend import db_backend

class SectFairylandRepositoryTests(unittest.TestCase):
    def test_upgrade_replays_and_rolls_assets_atomically(self):
        with tempfile.TemporaryDirectory() as temp:
            db=Path(temp)/'sect.db'
            with db_backend.transaction(db) as c:
                c.execute('CREATE TABLE user_xiuxian(user_id TEXT PRIMARY KEY,sect_id INTEGER,sect_position INTEGER)')
                c.execute("INSERT INTO user_xiuxian VALUES('owner',1,0)")
                c.execute('CREATE TABLE sects(sect_id INTEGER PRIMARY KEY,sect_owner TEXT,sect_fairyland INTEGER,sect_used_stone INTEGER,sect_materials INTEGER)')
                c.execute("INSERT INTO sects VALUES(1,'owner',1,1000,2000)")
            repo=SectFairylandSqlRepository(db)
            first=repo.upgrade('op','owner',1,1,2,100,200)
            duplicate=repo.upgrade('op','owner',1,1,2,100,200)
            self.assertEqual((first['status'],duplicate['status']),('upgraded','duplicate'))
            with db_backend.connection(db) as c:
                self.assertEqual(tuple(c.execute('SELECT sect_fairyland,sect_used_stone,sect_materials FROM sects').fetchone()),(2,900,1800))
