import tempfile
import unittest
from pathlib import Path
from ..repository import SectRenameSqlRepository
from tests.test_db_backend import db_backend
class SectDonationRepositoryTests(unittest.TestCase):
 def setUp(self):
  self.t=tempfile.TemporaryDirectory();self.db=Path(self.t.name)/'s.db'
  with db_backend.transaction(self.db) as c:
   c.execute('CREATE TABLE user_xiuxian(user_id TEXT PRIMARY KEY,sect_id INTEGER,stone INTEGER,sect_contribution INTEGER)');c.execute("INSERT INTO user_xiuxian VALUES('u',1,100,0)");c.execute('CREATE TABLE sects(sect_id INTEGER PRIMARY KEY,sect_used_stone INTEGER,sect_scale INTEGER,sect_materials INTEGER)');c.execute('INSERT INTO sects VALUES(1,0,0,0)');c.execute('CREATE TABLE sect_donation_operations(operation_id TEXT PRIMARY KEY,user_id TEXT,sect_id INTEGER,stone INTEGER,materials INTEGER)')
  self.r=SectRenameSqlRepository(self.db)
 def tearDown(self):self.t.cleanup()
 def test_success_duplicate_and_insufficient(self):
  a=self.r.donate('x','u',1,30,3);b=self.r.donate('x','u',1,99,9);c=self.r.donate('y','u',1,100,1);self.assertEqual(('donated','duplicate','stone_insufficient'),(a['status'],b['status'],c['status']))
if __name__=='__main__':unittest.main()
