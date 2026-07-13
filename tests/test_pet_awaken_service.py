import tempfile,unittest
from pathlib import Path
import nonebot
nonebot.init()
from nonebot_plugin_xiuxian_2.xiuxian.xiuxian_pet.awaken_service import PetAwakenService
from tests.test_db_backend import db_backend
class T(unittest.TestCase):
 def setUp(self):
  self.t=tempfile.TemporaryDirectory();r=Path(self.t.name);self.g=r/'g';self.p=r/'p'
  with db_backend.transaction(self.g) as c:c.execute('CREATE TABLE back(user_id TEXT,goods_id INTEGER,goods_num INTEGER)');c.execute("INSERT INTO back VALUES ('u',7,1)")
  with db_backend.transaction(self.p) as c:c.execute('CREATE TABLE player_pet_item(user_id TEXT,uid TEXT,skill_id TEXT)');c.execute("INSERT INTO player_pet_item VALUES ('u','x','old')")
  self.s=PetAwakenService(self.g,self.p)
 def tearDown(self):self.t.cleanup()
 def test_awaken(self):self.assertEqual(self.s.awaken('o','u','x','old','new',7).status,'applied');self.assertEqual(self.s.awaken('o','u','x','old','new',7).status,'duplicate')
 def test_stale(self):self.assertEqual(self.s.awaken('x','u','x','bad','new',7).status,'state_changed')
