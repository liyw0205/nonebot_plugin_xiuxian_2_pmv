import tempfile,unittest
from pathlib import Path
import nonebot
nonebot.init()
from nonebot_plugin_xiuxian_2.xiuxian.xiuxian_pet.release_service import PetReleaseService
from tests.test_db_backend import db_backend
class T(unittest.TestCase):
 def setUp(self):
  self.t=tempfile.TemporaryDirectory();r=Path(self.t.name);self.g=r/'g';self.p=r/'p'
  with db_backend.transaction(self.g) as c:c.execute('CREATE TABLE back(user_id TEXT,goods_id INTEGER,goods_name TEXT,goods_type TEXT,goods_num INTEGER,UNIQUE(user_id,goods_id))')
  with db_backend.transaction(self.p) as c:c.execute('CREATE TABLE player_pet(user_id TEXT PRIMARY KEY,active_uid TEXT,active TEXT)');c.execute("INSERT INTO player_pet VALUES ('u','x','x')");c.execute('CREATE TABLE player_pet_item(user_id TEXT,uid TEXT,total_exp INTEGER,is_active INTEGER)');c.execute("INSERT INTO player_pet_item VALUES ('u','x',100,1)")
  self.s=PetReleaseService(self.g,self.p)
 def tearDown(self):self.t.cleanup()
 def test_release(self):self.assertEqual(self.s.release('o','u','x',100,9,80,99).status,'applied');self.assertEqual(self.s.release('o','u','x',100,9,80,99).status,'duplicate')
 def test_stale(self):self.assertEqual(self.s.release('x','u','x',99,9,80,99).status,'state_changed')
