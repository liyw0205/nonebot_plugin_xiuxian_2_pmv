import tempfile,unittest
from pathlib import Path
import nonebot
nonebot.init()
from nonebot_plugin_xiuxian_2.xiuxian.xiuxian_lunhui.settlement_service import LunhuiSettlementService
from tests.test_db_backend import db_backend
class T(unittest.TestCase):
 def setUp(self):
  self.t=tempfile.TemporaryDirectory();self.d=Path(self.t.name)/'d'
  with db_backend.transaction(self.d) as c:c.execute('CREATE TABLE user_xiuxian(user_id TEXT PRIMARY KEY,level TEXT,exp INTEGER,stone INTEGER,level_up_rate INTEGER,root_type TEXT,root_level INTEGER)');c.execute("INSERT INTO user_xiuxian VALUES ('u','渡劫',999,200000000,5,'旧',1)");c.execute('CREATE TABLE back(user_id TEXT,goods_id INTEGER,goods_name TEXT,goods_type TEXT,goods_num INTEGER,UNIQUE(user_id,goods_id))')
  self.s=LunhuiSettlementService(self.d)
 def tearDown(self):self.t.cleanup()
 def test_settle(self):self.assertEqual(self.s.settle('o','u','渡劫','新',True,7,'卡').status,'applied');self.assertEqual(self.s.settle('o','u','渡劫','新',True,7,'卡').status,'duplicate')
 def test_stale(self):self.assertEqual(self.s.settle('x','u','错误','新',True,7,'卡').status,'state_changed')
