import tempfile
import unittest
from pathlib import Path
from ..repository import SectRenameSqlRepository
from tests.test_db_backend import db_backend
class SectSecbuffRepositoryTests(unittest.TestCase):
 def setUp(self):
  self.t=tempfile.TemporaryDirectory();self.db=Path(self.t.name)/'s.db';self._effects=False
  with db_backend.transaction(self.db) as c:
   c.execute('CREATE TABLE user_xiuxian(user_id TEXT PRIMARY KEY,sect_id INTEGER,sect_position INTEGER)');c.execute("INSERT INTO user_xiuxian VALUES('u',1,1)");c.execute('CREATE TABLE sects(sect_id INTEGER PRIMARY KEY,secbuff TEXT,sect_materials INTEGER)');c.execute("INSERT INTO sects VALUES(1,'2',50)");c.execute('CREATE TABLE BuffInfo(user_id TEXT PRIMARY KEY,sec_buff INTEGER)');c.execute("INSERT INTO BuffInfo VALUES('u',0)");c.execute('CREATE TABLE sect_secbuff_learn_operations(operation_id TEXT PRIMARY KEY,user_id TEXT,sect_id INTEGER,buff_id INTEGER,materials_cost INTEGER,materials_left INTEGER)')
  self.r=SectRenameSqlRepository(self.db)
 def tearDown(self):self.t.cleanup()
 def row(self,sql,params=()):
  with db_backend.connection(self.db) as c:return c.execute(sql,params).fetchone()
 def test_success_duplicate(self):
  a=self.r.learn_secondary('x','u',1,8,20,expected_catalog='2');b=self.r.learn_secondary('x','u',1,8,20,expected_catalog='2');self.assertEqual(('learned','duplicate'),(a['status'],b['status']))
 def _snapshot(self):
  return (self.row("SELECT sect_materials FROM sects WHERE sect_id=1"),self.row("SELECT sec_buff FROM BuffInfo WHERE user_id='u'"),self.row("SELECT COUNT(*) FROM sect_secbuff_learn_operations"),self.row("SELECT COUNT(*) FROM isolated_effect") if self._effects else None)
 def _effect_table(self):
  with db_backend.transaction(self.db) as c:c.execute("CREATE TABLE isolated_effect(name TEXT)")
  self._effects=True
 def test_receipt_ignore_rolls_back_with_specific_reason(self):
  self._effect_table()
  with db_backend.transaction(self.db) as c:c.execute("CREATE TRIGGER ignore_secbuff_receipt BEFORE INSERT ON sect_secbuff_learn_operations BEGIN INSERT INTO isolated_effect VALUES('receipt'); SELECT RAISE(IGNORE); END")
  before=self._snapshot(); result=self.r.learn_secondary('receipt-ignore','u',1,8,20,expected_catalog='2')
  self.assertEqual(('state_changed','sect_secbuff_receipt_write_rejected','receipt-ignore'),(result['status'],result['reason'],result['subject']))
  self.assertEqual(before,self._snapshot())
 def test_material_and_buff_ignore_roll_back(self):
  for name,verb,table,reason,subject in (("materials","UPDATE","sects","sect_secbuff_materials_write_rejected","1"),("buff","UPDATE","BuffInfo","sect_secbuff_buff_write_rejected","u")):
   with self.subTest(name=name):
    self._effect_table()
    with db_backend.transaction(self.db) as c:
     column="sect_materials" if table=="sects" else "sec_buff"
     c.execute(f"CREATE TRIGGER ignore_secbuff_{name} BEFORE UPDATE OF {column} ON {table} BEGIN INSERT INTO isolated_effect VALUES('{name}'); SELECT RAISE(IGNORE); END")
    before=self._snapshot(); result=self.r.learn_secondary('ignore-'+name,'u',1,8,20,expected_catalog='2')
    self.assertEqual(('state_changed',reason,subject),(result['status'],result['reason'],result['subject']))
    self.assertEqual(before,self._snapshot())
    with db_backend.transaction(self.db) as c:c.execute(f"DROP TRIGGER ignore_secbuff_{name}")
    self._effects=False
    with db_backend.transaction(self.db) as c:c.execute("DROP TABLE isolated_effect")
 def test_receipt_abort_is_transparent(self):
  with db_backend.transaction(self.db) as c:c.execute("CREATE TRIGGER abort_secbuff_receipt BEFORE INSERT ON sect_secbuff_learn_operations BEGIN SELECT RAISE(ABORT, 'secbuff receipt failed'); END")
  with self.assertRaises(db_backend.IntegrityError) as caught:self.r.learn_secondary('abort','u',1,8,20,expected_catalog='2')
  self.assertIn('secbuff receipt failed',str(caught.exception));self.assertEqual((50,0,0),(self.row("SELECT sect_materials FROM sects WHERE sect_id=1")[0],self.row("SELECT sec_buff FROM BuffInfo WHERE user_id='u'")[0],self.row("SELECT COUNT(*) FROM sect_secbuff_learn_operations")[0]))
if __name__=='__main__':unittest.main()
