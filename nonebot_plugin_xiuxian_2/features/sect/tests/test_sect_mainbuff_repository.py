import tempfile
import unittest
from pathlib import Path
from ..repository import SectRenameSqlRepository
from tests.test_db_backend import db_backend
class SectMainbuffRepositoryTests(unittest.TestCase):
 def setUp(self):
  self.t=tempfile.TemporaryDirectory();self.db=Path(self.t.name)/'s.db';self._has_effects=False
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
 def _snapshot(self):
  return (self.row("SELECT sect_materials FROM sects WHERE sect_id=1"),
          self.row("SELECT main_buff FROM BuffInfo WHERE user_id='u'") if self._has_effects else self.row("SELECT main_buff FROM BuffInfo"),
          self.row("SELECT COUNT(*) FROM sect_mainbuff_learn_operations"),
          self.row("SELECT COUNT(*) FROM isolated_effect") if self._has_effects else None)
 def _add_effect_table(self):
  with db_backend.transaction(self.db) as c:
   c.execute("CREATE TABLE isolated_effect(name TEXT)")
  self._has_effects=True
 def test_receipt_ignore_rolls_back_all_writes_and_trigger_effect(self):
  self._add_effect_table()
  with db_backend.transaction(self.db) as c:
   c.execute("CREATE TRIGGER ignore_mainbuff_receipt BEFORE INSERT ON sect_mainbuff_learn_operations BEGIN INSERT INTO isolated_effect VALUES('receipt'); SELECT RAISE(IGNORE); END")
  before=self._snapshot(); result=self.r.learn_main('receipt-ignore','u',1,7,20,expected_catalog='1')
  self.assertEqual(('state_changed','sect_mainbuff_receipt_write_rejected','receipt-ignore'),(result['status'],result['reason'],result['subject']))
  self.assertEqual(before,self._snapshot())
 def test_material_ignore_rolls_back_and_reports_sect_subject(self):
  self._add_effect_table()
  with db_backend.transaction(self.db) as c:
   c.execute("CREATE TRIGGER ignore_mainbuff_materials BEFORE UPDATE OF sect_materials ON sects BEGIN INSERT INTO isolated_effect VALUES('materials'); SELECT RAISE(IGNORE); END")
  before=self._snapshot(); result=self.r.learn_main('materials-ignore','u',1,7,20,expected_catalog='1')
  self.assertEqual(('state_changed','sect_mainbuff_materials_write_rejected','1'),(result['status'],result['reason'],result['subject']))
  self.assertEqual(before,self._snapshot())
 def test_buff_ignore_rolls_back_and_reports_player_subject(self):
  self._add_effect_table()
  with db_backend.transaction(self.db) as c:
   c.execute("CREATE TRIGGER ignore_mainbuff_buff BEFORE UPDATE OF main_buff ON BuffInfo BEGIN INSERT INTO isolated_effect VALUES('buff'); SELECT RAISE(IGNORE); END")
  before=self._snapshot(); result=self.r.learn_main('buff-ignore','u',1,7,20,expected_catalog='1')
  self.assertEqual(('state_changed','sect_mainbuff_buff_write_rejected','u'),(result['status'],result['reason'],result['subject']))
  self.assertEqual(before,self._snapshot())
 def test_receipt_rewrite_is_detected_and_can_recover_with_same_operation(self):
  self._add_effect_table()
  with db_backend.transaction(self.db) as c:
   c.execute("CREATE TRIGGER rewrite_mainbuff_receipt AFTER INSERT ON sect_mainbuff_learn_operations BEGIN UPDATE sect_mainbuff_learn_operations SET materials_left=0 WHERE operation_id=NEW.operation_id; INSERT INTO isolated_effect VALUES('rewrite'); END")
  before=self._snapshot(); result=self.r.learn_main('receipt-rewrite','u',1,7,20,expected_catalog='1')
  self.assertEqual(('state_changed','sect_mainbuff_receipt_write_rejected','receipt-rewrite'),(result['status'],result['reason'],result['subject']))
  self.assertEqual(before,self._snapshot())
  with db_backend.transaction(self.db) as c:
   c.execute("DROP TRIGGER rewrite_mainbuff_receipt")
  recovered=self.r.learn_main('receipt-rewrite','u',1,7,20,expected_catalog='1')
  self.assertEqual('learned',recovered['status'])
  self.assertEqual((30,7,1),(self.row("SELECT sect_materials FROM sects WHERE sect_id=1")[0],self.row("SELECT main_buff FROM BuffInfo WHERE user_id='u'")[0],self.row("SELECT COUNT(*) FROM sect_mainbuff_learn_operations")[0]))
 def test_receipt_abort_is_transparent_and_rolls_back(self):
  with db_backend.transaction(self.db) as c:
   c.execute("CREATE TRIGGER abort_mainbuff_receipt BEFORE INSERT ON sect_mainbuff_learn_operations BEGIN SELECT RAISE(ABORT, 'mainbuff receipt failed'); END")
  with self.assertRaises(db_backend.IntegrityError) as caught:
   self.r.learn_main('receipt-abort','u',1,7,20,expected_catalog='1')
  self.assertIn('mainbuff receipt failed',str(caught.exception))
  self.assertEqual((50,0,0),(self.row("SELECT sect_materials FROM sects WHERE sect_id=1")[0],self.row("SELECT main_buff FROM BuffInfo WHERE user_id='u'")[0],self.row("SELECT COUNT(*) FROM sect_mainbuff_learn_operations")[0]))
if __name__=='__main__':unittest.main()
