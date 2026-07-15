import tempfile
import unittest
from pathlib import Path
import nonebot
nonebot.init()
from nonebot_plugin_xiuxian_2.xiuxian.xiuxian_buff.transaction_service import PartnerBreakthroughService
from tests.test_db_backend import db_backend

class Tests(unittest.TestCase):
 def setUp(self):
  self.t=tempfile.TemporaryDirectory(); r=Path(self.t.name); self.g=r/'g'; self.p=r/'p'
  with db_backend.transaction(self.g) as c: c.execute('CREATE TABLE user_xiuxian(user_id TEXT PRIMARY KEY,exp INTEGER,power INTEGER)'); c.executemany('INSERT INTO user_xiuxian VALUES (%s,%s,0)',[('a',1000),('b',500)])
  with db_backend.transaction(self.p) as c: c.execute('CREATE TABLE partner(user_id TEXT PRIMARY KEY,partner_id TEXT,affection INTEGER)'); c.executemany('INSERT INTO partner VALUES (%s,%s,%s)',[('a','b',100),('b','a',50)])
  self.s=PartnerBreakthroughService(self.g,self.p)
 def tearDown(self): self.t.cleanup()
 def call(self,op='x',reward=10): return self.s.apply(op,'a','b','筑基',expected_user_exp=1000,expected_partner_exp=500,expected_affection=100,reward_exp=reward,partner_power=99)
 def test_idempotency_and_conflict(self):
  self.assertEqual('applied',self.call().status); self.assertEqual('duplicate',self.call().status); self.assertEqual('operation_conflict',self.call(reward=11).status)
  with db_backend.connection(self.g) as c: self.assertEqual((510,99),tuple(c.execute("SELECT exp,power FROM user_xiuxian WHERE user_id='b'").fetchone()))
 def test_snapshot_and_rollback(self):
  with db_backend.transaction(self.p) as c: c.execute("UPDATE partner SET affection=101 WHERE user_id='a'")
  self.assertEqual('state_changed',self.call().status)
  with db_backend.transaction(self.p) as c: c.execute("UPDATE partner SET affection=100 WHERE user_id='a'")
  self.call('seed')
  with db_backend.transaction(self.g) as c: c.execute("CREATE TRIGGER fail BEFORE INSERT ON partner_breakthrough_operations BEGIN SELECT RAISE(ABORT,'x'); END"); c.execute("UPDATE user_xiuxian SET exp=500,power=0 WHERE user_id='b'")
  with self.assertRaises(Exception): self.call('fail')
  with db_backend.connection(self.g) as c: self.assertEqual(500,c.execute("SELECT exp FROM user_xiuxian WHERE user_id='b'").fetchone()[0])
