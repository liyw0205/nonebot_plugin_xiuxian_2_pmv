import tempfile
import unittest
from pathlib import Path
import nonebot
nonebot.init()
from nonebot_plugin_xiuxian_2.xiuxian.xiuxian_buff.partner_cultivation_service import PartnerCultivationService
from tests.test_db_backend import db_backend

class Tests(unittest.TestCase):
 def setUp(self):
  self.t=tempfile.TemporaryDirectory(); r=Path(self.t.name); self.g=r/'g'; self.p=r/'p'
  with db_backend.transaction(self.g) as c:
   c.execute('CREATE TABLE user_xiuxian(user_id TEXT PRIMARY KEY,exp INTEGER,power INTEGER,hp INTEGER,mp INTEGER,atk INTEGER,level_up_rate INTEGER)'); c.executemany('INSERT INTO user_xiuxian VALUES (%s,%s,0,1,1,1,0)',[('a',1000),('b',2000)])
  with db_backend.transaction(self.p) as c:
   c.execute('CREATE TABLE partner(user_id TEXT PRIMARY KEY,partner_id TEXT,affection INTEGER)'); c.executemany('INSERT INTO partner VALUES (%s,%s,%s)',[('a','b',3),('b','a',4)])
  self.s=PartnerCultivationService(self.g,self.p)
 def tearDown(self): self.t.cleanup()
 def call(self,op='x',gain=100,aff=3): return self.s.apply(op,'a','b',expected_exp_1=1000,expected_exp_2=2000,exp_1=gain,exp_2=200,used_count=2,power_1=1,power_2=2,hp_1=3,mp_1=4,atk_1=5,hp_2=6,mp_2=7,atk_2=8,expected_affection_1=aff,expected_affection_2=4,affection_1=40,affection_2=20)
 def test_idempotency_and_conflict(self):
  self.assertEqual('applied',self.call().status); self.assertEqual('duplicate',self.call().status); self.assertEqual('operation_conflict',self.call(gain=101).status)
  with db_backend.connection(self.g) as c: self.assertEqual([1100,2200],[r[0] for r in c.execute('SELECT exp FROM user_xiuxian ORDER BY user_id').fetchall()])
 def test_snapshot_and_rollback(self):
  self.assertEqual('state_changed',self.call(aff=9).status)
  self.call('seed')
  with db_backend.transaction(self.g) as c: c.execute("CREATE TRIGGER fail BEFORE INSERT ON partner_cultivation_operations BEGIN SELECT RAISE(ABORT,'x'); END"); c.execute("UPDATE user_xiuxian SET exp=1000 WHERE user_id='a'"); c.execute("UPDATE user_xiuxian SET exp=2000 WHERE user_id='b'")
  with db_backend.transaction(self.p) as c: c.execute("UPDATE partner SET affection=3 WHERE user_id='a'"); c.execute("UPDATE partner SET affection=4 WHERE user_id='b'")
  with self.assertRaises(Exception): self.call('fail')
  with db_backend.connection(self.g) as c: self.assertEqual(1000,c.execute("SELECT exp FROM user_xiuxian WHERE user_id='a'").fetchone()[0])
