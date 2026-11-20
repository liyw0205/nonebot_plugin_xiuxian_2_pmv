import nonebot; nonebot.init()
import tempfile, unittest
from pathlib import Path
from nonebot_plugin_xiuxian_2.xiuxian.xiuxian_impart.transaction_service import ImpartDrawService
from nonebot_plugin_xiuxian_2.features.impart.card_bonus import BONUS_FIELDS
from tests.test_db_backend import db_backend
class T(unittest.TestCase):
 def test_impart_facade_no_longer_exposes_legacy_draw_service(self):
  from nonebot_plugin_xiuxian_2.xiuxian import xiuxian_impart
  self.assertFalse(hasattr(xiuxian_impart, "_impart_draw_service_instance"))

 def setUp(self):
  self.t=tempfile.TemporaryDirectory(); r=Path(self.t.name); self.g=r/'g'; self.i=r/'i'
  with db_backend.transaction(self.g) as c: c.execute('CREATE TABLE user_xiuxian(user_id TEXT,stone INTEGER)'); c.execute("INSERT INTO user_xiuxian VALUES ('u',100)")
  with db_backend.transaction(self.i) as c: c.execute('CREATE TABLE xiuxian_impart(user_id TEXT,wish INTEGER,impart_num INTEGER)'); c.execute("INSERT INTO xiuxian_impart VALUES ('u',0,0)"); c.execute('CREATE TABLE impart_cards(user_id TEXT,card_name TEXT,quantity INTEGER,PRIMARY KEY(user_id,card_name))')
  self.s=ImpartDrawService(self.g,self.i)
 def tearDown(self): self.t.cleanup()
 def test_draw(self): self.assertEqual(self.s.draw('o','u',100,0,0,20,10,2,['a']).status,'applied'); self.assertEqual(self.s.draw('o','u',100,0,0,20,10,2,['a']).status,'duplicate')
 def test_stale(self): self.assertEqual(self.s.draw('x','u',99,0,0,20,10,1,[]).status,'state_changed')
 def test_duplicate_history_is_not_updated(self):
  with db_backend.transaction(self.i) as c:
   c.execute("INSERT INTO xiuxian_impart VALUES('u',55,66)")
  self.assertEqual(self.s.draw('duplicate','u',100,0,0,20,10,2,['a']).status,'applied')
  with db_backend.connection(self.i) as c:
   self.assertEqual([tuple(row) for row in c.execute('SELECT wish,impart_num FROM xiuxian_impart ORDER BY rowid')],[(10,2),(55,66)])

 def test_separate_draw_batches_accumulate_duplicate_card_half_bonus(self):
  with tempfile.TemporaryDirectory() as temp:
   root=Path(temp); game=root/'game.db'; impart=root/'impart.db'
   bonus_columns=','.join(f'{field} REAL DEFAULT 0' for field in BONUS_FIELDS)
   with db_backend.transaction(game) as c:
    c.execute('CREATE TABLE user_xiuxian(user_id TEXT,stone INTEGER)')
    c.execute("INSERT INTO user_xiuxian VALUES('u',100)")
   with db_backend.transaction(impart) as c:
    c.execute(f'CREATE TABLE xiuxian_impart(user_id TEXT,wish INTEGER,impart_num INTEGER,{bonus_columns})')
    c.execute("INSERT INTO xiuxian_impart(user_id,wish,impart_num) VALUES('u',0,0)")
    c.execute('CREATE TABLE impart_cards(user_id TEXT,card_name TEXT,quantity INTEGER,PRIMARY KEY(user_id,card_name))')
   service=ImpartDrawService(game,impart)
   first=service.draw('first','u',100,0,0,20,10,2,['白玫瑰'])
   self.assertEqual(first.status,'applied')
   with db_backend.connection(impart) as c:
    self.assertAlmostEqual(c.execute('SELECT impart_atk_per FROM xiuxian_impart WHERE user_id=?',('u',)).fetchone()[0],0.1)
   second=service.draw('second','u',80,10,2,10,20,1,['白玫瑰'])
   self.assertEqual(second.status,'applied')
   with db_backend.connection(impart) as c:
    self.assertEqual(c.execute('SELECT quantity FROM impart_cards WHERE user_id=? AND card_name=?',('u','白玫瑰')).fetchone()[0],2)
    self.assertAlmostEqual(c.execute('SELECT impart_atk_per FROM xiuxian_impart WHERE user_id=?',('u',)).fetchone()[0],0.15)
