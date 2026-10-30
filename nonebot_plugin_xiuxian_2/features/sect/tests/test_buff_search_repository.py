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
                # 真实 sects 列是 mainbuff/secbuff（无 sect_mainbuff/sect_secbuff）。
                c.execute('CREATE TABLE sects(sect_id INTEGER PRIMARY KEY,sect_owner TEXT,mainbuff TEXT,secbuff TEXT,sect_used_stone INTEGER,sect_materials INTEGER)'); c.execute("INSERT INTO sects VALUES(1,'owner','[]','[]',1000,2000)")
            repo=SectBuffSearchSqlRepository(db)
            first=repo.apply('op','owner',1,'main','[]','[1,2]',100,200)
            duplicate=repo.apply('op','owner',1,'main','[]','[3]',999,999)
            self.assertEqual((first['status'],duplicate['status']),('applied','duplicate'))
            with db_backend.connection(db) as c:self.assertEqual(tuple(c.execute('SELECT mainbuff,sect_used_stone,sect_materials FROM sects').fetchone()),('[1,2]',900,1800))

    def test_owner_position_is_honoured_instead_of_a_hardcoded_zero(self):
        with tempfile.TemporaryDirectory() as temp:
            db=Path(temp)/'sect.db'
            with db_backend.transaction(db) as c:
                c.execute('CREATE TABLE user_xiuxian(user_id TEXT PRIMARY KEY,sect_id INTEGER,sect_position INTEGER)'); c.execute("INSERT INTO user_xiuxian VALUES('owner',1,3)")
                c.execute('CREATE TABLE sects(sect_id INTEGER PRIMARY KEY,sect_owner TEXT,mainbuff TEXT,secbuff TEXT,sect_used_stone INTEGER,sect_materials INTEGER)'); c.execute("INSERT INTO sects VALUES(1,'owner','[]','[]',1000,2000)")
            repo=SectBuffSearchSqlRepository(db)
            # 宗主位是 3 时也必须通过（旧实现对 owner_position 硬编码 0）。
            result=repo.apply('op','owner',1,'secondary','[]','[9]',10,20,owner_position=3)
            self.assertEqual(result['status'],'applied')
            # position 与传入值不符 -> not_owner 且零变化。
            denied=repo.apply('op2','owner',1,'secondary','[9]','[7]',10,20,owner_position=0)
            self.assertEqual(denied['status'],'not_owner')
            with db_backend.connection(db) as c:self.assertEqual(tuple(c.execute('SELECT secbuff,sect_used_stone,sect_materials FROM sects').fetchone()),('[9]',990,1980))
