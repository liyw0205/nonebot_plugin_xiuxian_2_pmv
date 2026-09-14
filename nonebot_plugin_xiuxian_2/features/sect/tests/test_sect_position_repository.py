import tempfile
import unittest
from pathlib import Path
from ..repository import SectRenameSqlRepository
from tests.test_db_backend import db_backend
class SectPositionRepositoryTests(unittest.TestCase):
 def setUp(self):
  self.t=tempfile.TemporaryDirectory();self.db=Path(self.t.name)/'s.db'
  with db_backend.transaction(self.db) as c:
   c.execute('CREATE TABLE user_xiuxian(user_id TEXT PRIMARY KEY,sect_id INTEGER,sect_position INTEGER,user_name TEXT)');c.execute("INSERT INTO user_xiuxian VALUES('elder',1,2,'长老')");c.execute("INSERT INTO user_xiuxian VALUES('member',1,5,'成员')");c.execute('CREATE TABLE sect_position_change_operations(operation_id TEXT PRIMARY KEY,actor_id TEXT,target_id TEXT,sect_id INTEGER,actor_name TEXT,target_name TEXT,old_position INTEGER,new_position INTEGER)')
  self.r=SectRenameSqlRepository(self.db)
 def tearDown(self):self.t.cleanup()
 def test_change_duplicate_and_invalid_rank(self):
  a=self.r.change_position('x','elder','member',4,{4:2,5:3},manager_max_position=2);b=self.r.change_position('x','elder','member',5,{4:2,5:3},manager_max_position=2);c=self.r.change_position('y','member','elder',4,{4:2,5:3},manager_max_position=2);self.assertEqual(('changed','duplicate','actor_not_manager'),(a['status'],b['status'],c['status']))
if __name__=='__main__':unittest.main()
