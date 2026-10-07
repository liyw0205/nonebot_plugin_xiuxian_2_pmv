import tempfile
import unittest
from pathlib import Path
from ..repository import SectRenameSqlRepository
from tests.test_db_backend import db_backend
class SectMainbuffRepositoryTests(unittest.TestCase):
 def setUp(self):
  self.t=tempfile.TemporaryDirectory();self.db=Path(self.t.name)/'s.db'
  with db_backend.transaction(self.db) as c:
   c.execute('CREATE TABLE user_xiuxian(user_id TEXT PRIMARY KEY,sect_id INTEGER,sect_position INTEGER)');c.execute("INSERT INTO user_xiuxian VALUES('u',1,1)");c.execute('CREATE TABLE sects(sect_id INTEGER PRIMARY KEY,mainbuff TEXT,sect_materials INTEGER)');c.execute("INSERT INTO sects VALUES(1,'1',50)");c.execute('CREATE TABLE BuffInfo(user_id TEXT PRIMARY KEY,main_buff INTEGER)');c.execute("INSERT INTO BuffInfo VALUES('u',0)");c.execute('CREATE TABLE sect_mainbuff_learn_operations(operation_id TEXT PRIMARY KEY,user_id TEXT,sect_id INTEGER,buff_id INTEGER,materials_cost INTEGER,materials_left INTEGER)')
  self.r=SectRenameSqlRepository(self.db)
 def tearDown(self):self.t.cleanup()
 def row(self, sql, params=()):
  with db_backend.connection(self.db) as conn:
   return conn.execute(sql, params).fetchone()
 def test_success_duplicate_and_materials(self):
  a=self.r.learn_main('x','u',1,7,20,expected_catalog='1');b=self.r.learn_main('x','u',1,7,20,expected_catalog='1');self.assertEqual(('learned','duplicate'),(a['status'],b['status']))
 def test_negative_material_cost_is_rejected_without_writes(self):
  result=self.r.learn_main('negative','u',1,7,-20,expected_catalog='1')
  self.assertEqual(result['status'],'invalid_materials')
  self.assertEqual(self.row("SELECT sect_materials FROM sects WHERE sect_id=1")[0],50)
  self.assertIsNone(self.row("SELECT 1 FROM sect_mainbuff_learn_operations WHERE operation_id='negative'"))
 def test_compare_and_set_mismatch_rolls_back_both_updates(self):
  with db_backend.transaction(self.db) as c:
   c.execute('ALTER TABLE BuffInfo RENAME TO BuffInfo_base')
   c.execute('CREATE VIEW BuffInfo AS SELECT * FROM BuffInfo_base')
   c.execute('CREATE TRIGGER noop_buff_update INSTEAD OF UPDATE ON BuffInfo BEGIN SELECT 1; END')
  result=self.r.learn_main('cas','u',1,7,20,expected_catalog='1')
  self.assertEqual(result['status'],'state_changed')
  self.assertEqual(self.row("SELECT sect_materials FROM sects WHERE sect_id=1")[0],50)
  self.assertEqual(self.row("SELECT main_buff FROM BuffInfo_base WHERE user_id='u'")[0],0)
if __name__=='__main__':unittest.main()
