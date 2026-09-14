import tempfile
import unittest
from pathlib import Path
from ..repository import SectRenameSqlRepository
from tests.test_db_backend import db_backend
class SectKickRepositoryTests(unittest.TestCase):
 def setUp(self):
  self.t=tempfile.TemporaryDirectory();self.db=Path(self.t.name)/'s.db'
  with db_backend.transaction(self.db) as c:
   c.execute('CREATE TABLE user_xiuxian(user_id TEXT PRIMARY KEY,sect_id INTEGER,sect_position INTEGER,sect_contribution INTEGER,user_name TEXT)');c.execute("INSERT INTO user_xiuxian VALUES('elder',1,2,0,'长老')");c.execute("INSERT INTO user_xiuxian VALUES('member',1,5,8,'成员')");c.execute('CREATE TABLE sects(sect_id INTEGER PRIMARY KEY,sect_name TEXT)');c.execute("INSERT INTO sects VALUES(1,'宗门')");c.execute('CREATE TABLE sect_member_removal_operations(operation_id TEXT PRIMARY KEY,operation_type TEXT,actor_id TEXT,target_id TEXT,sect_id INTEGER,sect_name TEXT,actor_name TEXT,target_name TEXT,actor_position INTEGER,target_position INTEGER)')
  self.r=SectRenameSqlRepository(self.db)
 def tearDown(self):self.t.cleanup()
 def test_success_duplicate_and_rank(self):
  a=self.r.kick('x','elder','member',manager_max_position=2);b=self.r.kick('x','elder','member',manager_max_position=2);self.assertEqual(('kicked','duplicate'),(a['status'],b['status']))
if __name__=='__main__':unittest.main()
