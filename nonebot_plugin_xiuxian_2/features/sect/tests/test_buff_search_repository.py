import tempfile
import unittest
from pathlib import Path
from ..buff_search_repository import SectBuffSearchSqlRepository
from tests.test_db_backend import db_backend

class SectBuffSearchRepositoryTests(unittest.TestCase):
    def test_search_is_idempotent_and_deducts_assets(self):
        with tempfile.TemporaryDirectory() as temp:
            db=Path(temp)/'sect.db'
            with db_backend.transaction(db) as c:
                c.execute('CREATE TABLE user_xiuxian(user_id TEXT PRIMARY KEY,sect_id INTEGER,sect_position INTEGER)'); c.execute("INSERT INTO user_xiuxian VALUES('owner',1,0)")
                c.execute('CREATE TABLE sects(sect_id INTEGER PRIMARY KEY,sect_owner TEXT,sect_mainbuff TEXT,sect_secbuff TEXT,sect_used_stone INTEGER,sect_materials INTEGER)'); c.execute("INSERT INTO sects VALUES(1,'owner','[]','[]',1000,2000)")
            repo=SectBuffSearchSqlRepository(db)
            first=repo.apply('op','owner',1,'main','[]','[1,2]',100,200)
            duplicate=repo.apply('op','owner',1,'main','[]','[3]',999,999)
            self.assertEqual((first['status'],duplicate['status']),('applied','duplicate'))
            with db_backend.connection(db) as c:self.assertEqual(tuple(c.execute('SELECT sect_mainbuff,sect_used_stone,sect_materials FROM sects').fetchone()),('[1,2]',900,1800))
