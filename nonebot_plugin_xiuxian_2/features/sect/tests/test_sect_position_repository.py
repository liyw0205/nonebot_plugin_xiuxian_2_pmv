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
  a=self.r.change_position('x','elder','member',4,{4:2,5:3},manager_max_position=2);b=self.r.change_position('x','elder','member',4,{4:2,5:3},manager_max_position=2);c=self.r.change_position('y','member','elder',4,{4:2,5:3},manager_max_position=2);self.assertEqual(('changed','duplicate','actor_not_manager'),(a['status'],b['status'],c['status']))

 def test_changed_operation_payload_is_conflict(self):
  self.assertEqual('changed', self.r.change_position('x','elder','member',4,{4:2,5:3},manager_max_position=2)['status'])
  result=self.r.change_position('x','elder','member',5,{4:2,5:3},manager_max_position=2)
  self.assertEqual(('operation_conflict','operation_conflict','actor'),(result['status'],result['reason'],result['subject']))

 def test_receipt_ignore_rolls_back_position_and_reports_reason(self):
  with db_backend.transaction(self.db) as c:
   c.execute("CREATE TRIGGER position_receipt_ignore BEFORE INSERT ON sect_position_change_operations BEGIN SELECT RAISE(IGNORE); END")
  result=self.r.change_position('ignore','elder','member',4,{4:2,5:3},manager_max_position=2)
  self.assertEqual(('state_changed','sect_position_receipt_write_rejected','ignore'),(result['status'],result['reason'],result['subject']))
  with db_backend.connection(self.db) as c:
   self.assertEqual((c.execute("SELECT sect_position FROM user_xiuxian WHERE user_id='member'").fetchone()[0],c.execute('SELECT COUNT(*) FROM sect_position_change_operations').fetchone()[0]),(5,0))

 def test_member_trigger_rewrite_rolls_back_position_and_receipt(self):
  with db_backend.transaction(self.db) as c:
   c.execute("CREATE TRIGGER position_rewrite AFTER UPDATE OF sect_position ON user_xiuxian WHEN NEW.user_id='member' BEGIN UPDATE user_xiuxian SET sect_position=3 WHERE user_id='member'; END")
  result=self.r.change_position('rewrite','elder','member',4,{4:2,5:3},manager_max_position=2)
  self.assertEqual(('state_changed','sect_position_member_write_rejected','member'),(result['status'],result['reason'],result['subject']))
  with db_backend.connection(self.db) as c:
   self.assertEqual((c.execute("SELECT sect_position FROM user_xiuxian WHERE user_id='member'").fetchone()[0],c.execute('SELECT COUNT(*) FROM sect_position_change_operations').fetchone()[0]),(5,0))

 def test_receipt_trigger_rewrite_rolls_back_and_recovers_with_same_operation(self):
  with db_backend.transaction(self.db) as c:
   c.execute("CREATE TRIGGER receipt_rewrite AFTER INSERT ON sect_position_change_operations BEGIN UPDATE sect_position_change_operations SET new_position=3 WHERE operation_id=NEW.operation_id; END")
  failed=self.r.change_position('recover','elder','member',4,{4:2,5:3},manager_max_position=2)
  self.assertEqual('sect_position_receipt_write_rejected', failed['reason'])
  with db_backend.transaction(self.db) as c:
   c.execute('DROP TRIGGER receipt_rewrite')
  recovered=self.r.change_position('recover','elder','member',4,{4:2,5:3},manager_max_position=2)
  self.assertEqual('changed', recovered['status'])
  with db_backend.connection(self.db) as c:
   self.assertEqual((c.execute("SELECT sect_position FROM user_xiuxian WHERE user_id='member'").fetchone()[0],c.execute('SELECT COUNT(*) FROM sect_position_change_operations').fetchone()[0]),(4,1))

 def test_receipt_abort_preserves_original_exception_and_rolls_back(self):
  with db_backend.transaction(self.db) as c:
   c.execute("CREATE TRIGGER position_receipt_abort BEFORE INSERT ON sect_position_change_operations BEGIN SELECT RAISE(ABORT, 'receipt boom'); END")
  with self.assertRaisesRegex(db_backend.Error, 'receipt boom'):
   self.r.change_position('abort','elder','member',4,{4:2,5:3},manager_max_position=2)
  with db_backend.connection(self.db) as c:
   self.assertEqual((c.execute("SELECT sect_position FROM user_xiuxian WHERE user_id='member'").fetchone()[0],c.execute('SELECT COUNT(*) FROM sect_position_change_operations').fetchone()[0]),(5,0))
if __name__=='__main__':unittest.main()
