import tempfile,unittest
from pathlib import Path
import nonebot
nonebot.init()
from nonebot_plugin_xiuxian_2.xiuxian.xiuxian_rift.speedup_service import RiftSpeedupService
from tests.test_db_backend import db_backend
class T(unittest.TestCase):
 def setUp(self):
  self.t=tempfile.TemporaryDirectory();self.d=Path(self.t.name)/'d';
  with db_backend.transaction(self.d) as c:c.execute('CREATE TABLE rift_entries(user_id TEXT,status TEXT,duration INTEGER)');c.execute("INSERT INTO rift_entries VALUES ('u','active',20)");c.execute('CREATE TABLE user_cd(user_id TEXT,scheduled_time INTEGER)');c.execute("INSERT INTO user_cd VALUES ('u',20)");c.execute('CREATE TABLE back(user_id TEXT,goods_id INTEGER,goods_num INTEGER)');c.execute("INSERT INTO back VALUES ('u',7,1)")
  self.s=RiftSpeedupService(self.d)
 def tearDown(self):self.t.cleanup()
 def test_all(self):
  self.assertEqual(self.s.apply('x','u',7,20,10).status,'applied');self.assertEqual(self.s.apply('x','u',7,20,10).status,'duplicate');self.assertEqual(self.s.apply('y','u',7,20,10).status,'state_changed')
