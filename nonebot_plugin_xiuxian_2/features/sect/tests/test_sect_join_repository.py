import tempfile
import unittest
from pathlib import Path
from ..repository import SectRenameSqlRepository
from tests.test_db_backend import db_backend
class SectJoinRepositoryTests(unittest.TestCase):
 def setUp(self):
  self.t=tempfile.TemporaryDirectory();self.db=Path(self.t.name)/'s.db'
  with db_backend.transaction(self.db) as c:
   c.execute('CREATE TABLE user_xiuxian(user_id TEXT PRIMARY KEY,sect_id INTEGER,sect_position INTEGER)');c.execute("INSERT INTO user_xiuxian VALUES('new',NULL,NULL)");c.execute('CREATE TABLE sects(sect_id INTEGER PRIMARY KEY,sect_name TEXT,sect_scale INTEGER,join_open INTEGER,closed INTEGER)');c.execute("INSERT INTO sects VALUES(1,'宗门',0,1,0)");c.execute('CREATE TABLE sect_member_join_operations(operation_id TEXT PRIMARY KEY,user_id TEXT,sect_id INTEGER,member_count INTEGER,member_limit INTEGER)')
  self.r=SectRenameSqlRepository(self.db)
 def tearDown(self):self.t.cleanup()
 def test_success_duplicate_and_conflict(self):
  a=self.r.join('x','new',1);b=self.r.join('x','new',1);c=self.r.join('x','other',1);self.assertEqual((a['status'],b['status'],c['status']),('joined','duplicate','operation_conflict'))
if __name__=='__main__':unittest.main()
