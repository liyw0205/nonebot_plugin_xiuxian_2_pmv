import nonebot; nonebot.init()
import tempfile, unittest
from pathlib import Path
from nonebot_plugin_xiuxian_2.xiuxian.xiuxian_pet.feed_service import PetFeedService
from tests.test_db_backend import db_backend
class T(unittest.TestCase):
 def setUp(self):
  self.t=tempfile.TemporaryDirectory(); r=Path(self.t.name); self.g=r/'g'; self.p=r/'p'
  with db_backend.transaction(self.g) as c: c.execute('CREATE TABLE back(user_id TEXT,goods_id INTEGER,goods_num INTEGER)'); c.execute("INSERT INTO back VALUES ('u',7,3)")
  with db_backend.transaction(self.p) as c: c.execute('CREATE TABLE player_pet_item(user_id TEXT,uid TEXT,stars INTEGER,exp INTEGER,total_exp INTEGER,is_active INTEGER,updated_at INTEGER)'); c.execute("INSERT INTO player_pet_item VALUES ('u','x',1,0,0,1,0)")
  self.s=PetFeedService(self.g,self.p)
 def tearDown(self): self.t.cleanup()
 def test_feed(self): self.assertEqual(self.s.feed('o','u','x',7,2,(1,0,0),(2,5,15)).status,'applied'); self.assertEqual(self.s.feed('o','u','x',7,2,(1,0,0),(2,5,15)).status,'duplicate')
 def test_missing(self): self.assertEqual(self.s.feed('x','u','x',7,4,(1,0,0),(2,5,15)).status,'item_missing')
