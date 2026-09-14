import tempfile
import unittest
from pathlib import Path
from ..repository import SectRenameSqlRepository
from tests.test_db_backend import db_backend
class SectSecbuffRepositoryTests(unittest.TestCase):
 def setUp(self):
  self.t=tempfile.TemporaryDirectory();self.db=Path(self.t.name)/'s.db'
  with db_backend.transaction(self.db) as c:
   c.execute('CREATE TABLE user_xiuxian(user_id TEXT PRIMARY KEY,sect_id INTEGER,sect_position INTEGER)');c.execute("INSERT INTO user_xiuxian VALUES('u',1,1)");c.execute('CREATE TABLE sects(sect_id INTEGER PRIMARY KEY,secbuff TEXT,sect_materials INTEGER)');c.execute("INSERT INTO sects VALUES(1,'2',50)");c.execute('CREATE TABLE BuffInfo(user_id TEXT PRIMARY KEY,sec_buff INTEGER)');c.execute("INSERT INTO BuffInfo VALUES('u',0)");c.execute('CREATE TABLE sect_secbuff_learn_operations(operation_id TEXT PRIMARY KEY,user_id TEXT,sect_id INTEGER,buff_id INTEGER,materials_cost INTEGER,materials_left INTEGER)')
  self.r=SectRenameSqlRepository(self.db)
 def tearDown(self):self.t.cleanup()
 def test_success_duplicate(self):
  a=self.r.learn_secondary('x','u',1,8,20,expected_catalog='2');b=self.r.learn_secondary('x','u',1,8,20,expected_catalog='2');self.assertEqual(('learned','duplicate'),(a['status'],b['status']))
if __name__=='__main__':unittest.main()
