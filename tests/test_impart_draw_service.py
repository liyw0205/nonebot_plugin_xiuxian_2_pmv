import nonebot; nonebot.init()
import tempfile, unittest
from pathlib import Path
from nonebot_plugin_xiuxian_2.xiuxian.xiuxian_impart.transaction_service import ImpartDrawService
from tests.test_db_backend import db_backend
class T(unittest.TestCase):
 def setUp(self):
  self.t=tempfile.TemporaryDirectory(); r=Path(self.t.name); self.g=r/'g'; self.i=r/'i'
  with db_backend.transaction(self.g) as c: c.execute('CREATE TABLE user_xiuxian(user_id TEXT,stone INTEGER)'); c.execute("INSERT INTO user_xiuxian VALUES ('u',100)")
  with db_backend.transaction(self.i) as c: c.execute('CREATE TABLE xiuxian_impart(user_id TEXT,wish INTEGER,impart_num INTEGER)'); c.execute("INSERT INTO xiuxian_impart VALUES ('u',0,0)"); c.execute('CREATE TABLE impart_cards(user_id TEXT,card_name TEXT,quantity INTEGER,PRIMARY KEY(user_id,card_name))')
  self.s=ImpartDrawService(self.g,self.i)
 def tearDown(self): self.t.cleanup()
 def test_draw(self): self.assertEqual(self.s.draw('o','u',100,0,0,20,10,2,['a']).status,'applied'); self.assertEqual(self.s.draw('o','u',100,0,0,20,10,2,['a']).status,'duplicate')
 def test_stale(self): self.assertEqual(self.s.draw('x','u',99,0,0,20,10,1,[]).status,'state_changed')
