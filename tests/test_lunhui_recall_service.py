import nonebot; nonebot.init()
import tempfile, unittest
from pathlib import Path
from nonebot_plugin_xiuxian_2.xiuxian.xiuxian_lunhui.recall_service import LunhuiRecallService
from tests.test_db_backend import db_backend
class T(unittest.TestCase):
 def setUp(self):
  self.t=tempfile.TemporaryDirectory(); r=Path(self.t.name); self.g=r/'g'; self.p=r/'p'
  with db_backend.transaction(self.g) as c: c.execute('CREATE TABLE BuffInfo(user_id TEXT,main_buff INTEGER,sub_buff INTEGER,sec_buff INTEGER,effect1_buff INTEGER,effect2_buff INTEGER)'); c.execute("INSERT INTO BuffInfo VALUES ('u',0,0,0,0,0)")
  with db_backend.transaction(self.p) as c: c.execute('CREATE TABLE reincarnation_memory(user_id TEXT,main_buff INTEGER,retrieved_main INTEGER)'); c.execute("INSERT INTO reincarnation_memory VALUES ('u',7,0)")
  self.s=LunhuiRecallService(self.g,self.p)
 def tearDown(self): self.t.cleanup()
 def test_recall(self):
  self.assertEqual(self.s.recall('o','u','main_buff',7).status,'applied')
  # mutable expected skill_id must not break same-op replay when type matches
  self.assertEqual(self.s.recall('o','u','main_buff',9).status,'duplicate')
  self.assertIsNotNone(self.s.get_result('o'))
 def test_stale(self): self.assertEqual(self.s.recall('x','u','main_buff',8).status,'state_changed')
