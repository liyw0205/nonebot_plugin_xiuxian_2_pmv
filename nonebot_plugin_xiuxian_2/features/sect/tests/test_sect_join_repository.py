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
 def row(self, sql):
  with db_backend.connection(self.db) as c:
   row=c.execute(sql).fetchone()
   return None if row is None else tuple(row)
 def test_success_duplicate_and_conflict(self):
  a=self.r.join('x','new',1);b=self.r.join('x','new',1);c=self.r.join('x','other',1);self.assertEqual((a['status'],b['status'],c['status']),('joined','duplicate','operation_conflict'));self.assertEqual(b['sect_name'],'宗门')

 def test_member_rewrite_rolls_back_and_recovers(self):
  with db_backend.transaction(self.db) as c:
   c.execute("CREATE TRIGGER rewrite_member AFTER UPDATE OF sect_id ON user_xiuxian BEGIN UPDATE user_xiuxian SET sect_position=4 WHERE user_id='new'; END")
  failed=self.r.join('rewrite-member','new',1,member_position=12)
  self.assertEqual((failed['status'],failed['reason'],failed['subject']),('state_changed','sect_member_join_member_write_rejected','new'))
  self.assertEqual(self.row("SELECT sect_id,sect_position FROM user_xiuxian WHERE user_id='new'"), (None,None))
  self.assertEqual(self.row("SELECT COUNT(*) FROM sect_member_join_operations"), (0,))
  with db_backend.transaction(self.db) as c:c.execute('DROP TRIGGER rewrite_member')
  self.assertEqual(self.r.join('rewrite-member','new',1,member_position=12)['status'],'joined')

 def test_state_rewrite_rolls_back_and_recovers(self):
  with db_backend.transaction(self.db) as c:
   c.execute("CREATE TRIGGER rewrite_state AFTER UPDATE OF sect_id ON user_xiuxian BEGIN UPDATE sects SET join_open=0 WHERE sect_id=1; END")
  failed=self.r.join('rewrite-state','new',1,member_position=12)
  self.assertEqual((failed['status'],failed['reason'],failed['subject']),('state_changed','sect_member_join_state_write_rejected','sect'))
  self.assertEqual(self.row("SELECT sect_id,sect_position FROM user_xiuxian WHERE user_id='new'"), (None,None))
  with db_backend.transaction(self.db) as c:c.execute('DROP TRIGGER rewrite_state')
  self.assertEqual(self.r.join('rewrite-state','new',1,member_position=12)['status'],'joined')

 def test_receipt_ignore_and_rewrite_roll_back_and_recover(self):
  for name, trigger in (
   ('ignore-receipt', "CREATE TRIGGER ignore_receipt BEFORE INSERT ON sect_member_join_operations BEGIN SELECT RAISE(IGNORE); END"),
   ('rewrite-receipt', "CREATE TRIGGER rewrite_receipt AFTER INSERT ON sect_member_join_operations BEGIN UPDATE sect_member_join_operations SET user_id='tampered' WHERE operation_id='rewrite-receipt'; END"),
  ):
   with self.subTest(name=name):
    with db_backend.transaction(self.db) as c:c.execute(trigger)
    failed=self.r.join(name,'new',1,member_position=12)
    self.assertEqual((failed['status'],failed['reason'],failed['subject']),('state_changed','sect_member_join_receipt_write_rejected',name))
    self.assertEqual(self.row("SELECT sect_id,sect_position FROM user_xiuxian WHERE user_id='new'"), (None,None))
    self.assertEqual(self.row("SELECT COUNT(*) FROM sect_member_join_operations"), (0,))
    with db_backend.transaction(self.db) as c:c.execute('DROP TRIGGER ' + ('ignore_receipt' if name == 'ignore-receipt' else 'rewrite_receipt'))
    self.assertEqual(self.r.join(name,'new',1,member_position=12)['status'],'joined')
    with db_backend.transaction(self.db) as c:
     c.execute("UPDATE user_xiuxian SET sect_id=NULL,sect_position=NULL WHERE user_id='new'")
     c.execute('DELETE FROM sect_member_join_operations')

 def test_receipt_abort_is_transparent_and_rolls_back(self):
  with db_backend.transaction(self.db) as c:
   c.execute("CREATE TRIGGER abort_receipt BEFORE INSERT ON sect_member_join_operations BEGIN SELECT RAISE(ABORT, 'join receipt failed'); END")
  with self.assertRaises(db_backend.IntegrityError):self.r.join('abort-receipt','new',1,member_position=12)
  self.assertEqual(self.row("SELECT sect_id,sect_position FROM user_xiuxian WHERE user_id='new'"), (None,None))
if __name__=='__main__':unittest.main()
