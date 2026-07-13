import tempfile,unittest
from pathlib import Path
import nonebot
nonebot.init()
from nonebot_plugin_xiuxian_2.xiuxian.xiuxian_rift.entry_service import RiftEntryService
from tests.test_db_backend import db_backend
class RiftEntryServiceTests(unittest.TestCase):
 def setUp(self):
  self.t=tempfile.TemporaryDirectory();self.db=Path(self.t.name)/'g.db'
  with db_backend.transaction(self.db) as c:c.execute('CREATE TABLE user_xiuxian(user_id TEXT PRIMARY KEY)');c.execute("INSERT INTO user_xiuxian VALUES ('u')");c.execute('CREATE TABLE user_cd(user_id TEXT PRIMARY KEY,type INTEGER,create_time TEXT,scheduled_time TEXT)');c.execute("INSERT INTO user_cd VALUES ('u',0,NULL,NULL)");c.execute('CREATE TABLE back(user_id TEXT,goods_id INTEGER,goods_num INTEGER)');c.execute("INSERT INTO back VALUES ('u',7,1)")
  self.s=RiftEntryService(self.db)
 def tearDown(self):self.t.cleanup()
 def test_entry_duplicate_and_ticket(self):
  self.assertEqual(self.s.enter('x','u','r',{'rank':1},10,7).status,'applied');self.assertEqual(self.s.enter('x','u','r',{'rank':1},10,7).status,'duplicate')
  with db_backend.connection(self.db) as c:self.assertEqual(int(c.execute("SELECT goods_num FROM back").fetchone()[0]),0)
 def test_reject_busy_and_missing_ticket(self):
  self.assertEqual(self.s.enter('a','u','r',{},10,8).status,'ticket_missing')
  with db_backend.transaction(self.db) as c:c.execute("UPDATE user_cd SET type=1")
  self.assertEqual(self.s.enter('b','u','r',{},10).status,'busy')
 def test_failure_rolls_back(self):
  with db_backend.transaction(self.db) as c:c.execute('CREATE TABLE rift_entry_operations(operation_id TEXT PRIMARY KEY,payload TEXT,entry_count INTEGER,created_at TIMESTAMP)');c.execute("CREATE TRIGGER fail BEFORE INSERT ON rift_entry_operations BEGIN SELECT RAISE(ABORT,'x');END")
  with self.assertRaises(db_backend.IntegrityError):self.s.enter('z','u','r',{},10,7)
  with db_backend.connection(self.db) as c:self.assertEqual(int(c.execute('SELECT goods_num FROM back').fetchone()[0]),1)
