import tempfile
import unittest
from pathlib import Path
from ..repository import SectRenameSqlRepository
from tests.test_db_backend import db_backend
class SectRenameRepositoryTests(unittest.TestCase):
 def setUp(self):
  self.t=tempfile.TemporaryDirectory();self.db=Path(self.t.name)/'s.db'
  with db_backend.transaction(self.db) as c:
   c.execute('CREATE TABLE user_xiuxian(user_id TEXT PRIMARY KEY,sect_id INTEGER,sect_position INTEGER)');c.execute("INSERT INTO user_xiuxian VALUES('owner',1,0)");c.execute('CREATE TABLE sects(sect_id INTEGER PRIMARY KEY,sect_name TEXT,sect_owner TEXT,sect_used_stone INTEGER)');c.execute("INSERT INTO sects VALUES(1,'旧宗','owner',1000)");c.execute('CREATE TABLE back(user_id TEXT,goods_id INTEGER,goods_num INTEGER,bind_num INTEGER)');c.execute("INSERT INTO back VALUES('owner',1999,2,1)");c.execute('CREATE TABLE sect_rename_operations(operation_id TEXT PRIMARY KEY,payload TEXT,previous_name TEXT,new_name TEXT)')
  self.r=SectRenameSqlRepository(self.db)
 def tearDown(self):self.t.cleanup()
 def test_success_duplicate_and_conflict(self):
  a=self.r.rename('x','owner',1,'新宗',300,1999);b=self.r.rename('x','owner',1,'别名',999,9999);c=self.r.rename('y','owner',1,'新宗',300,1999);self.assertEqual((a['status'],b['status'],c['status']),('renamed','duplicate','name_exists'))
if __name__=='__main__':unittest.main()
