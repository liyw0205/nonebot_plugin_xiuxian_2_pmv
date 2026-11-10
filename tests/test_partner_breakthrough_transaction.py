import tempfile
import unittest
from pathlib import Path
import nonebot
nonebot.init()
from nonebot_plugin_xiuxian_2.xiuxian.xiuxian_buff.transaction_service import PartnerBreakthroughService
from tests.test_db_backend import db_backend
from nonebot_plugin_xiuxian_2.core.numeric import as_int_like

class Tests(unittest.TestCase):
 def test_partner_facade_defers_breakthrough_service_construction(self):
  from nonebot_plugin_xiuxian_2.xiuxian.xiuxian_buff import partner
  self.assertIsNone(partner._partner_breakthrough_service_instance)

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

 def legacy(self,affection='1e2'):
  with db_backend.transaction(self.p) as c:
   c.execute('DROP TABLE partner')
   c.execute('CREATE TABLE partner(user_id TEXT PRIMARY KEY,partner_id TEXT,affection TEXT)')
   c.executemany('INSERT INTO partner VALUES (%s,%s,%s)',[('a','b',affection),('b','a','5e1')])

 def snapshot(self):
  result=[]
  for database in (self.g,self.p):
   with db_backend.connection(database) as c:
    tables=c.execute("SELECT name FROM sqlite_master WHERE type='table' ORDER BY name").fetchall()
    result.append({row[0]:[tuple(item) for item in c.execute(f'SELECT * FROM "{row[0]}" ORDER BY rowid').fetchall()] for row in tables})
  return result

 def test_legacy_scientific_affection_and_exp_receipt(self):
  self.legacy()
  self.assertEqual('applied',self.call().status)
  settled=self.snapshot()
  self.assertEqual('duplicate',self.call().status)
  self.assertEqual(settled,self.snapshot())
  with db_backend.transaction(self.g) as c:
   c.execute("UPDATE partner_breakthrough_operations SET reward_exp='1e29'")
  replay=self.call()
  self.assertEqual(as_int_like(1e29),replay.reward_exp)

 def test_precise_affection_change_and_receipt_failure_roll_back(self):
  self.legacy('100000000000000000001')
  before=self.snapshot()
  changed=self.s.apply('changed','a','b','筑基',expected_user_exp=1000,expected_partner_exp=500,expected_affection=100000000000000000000,reward_exp=10,partner_power=99)
  self.assertEqual('state_changed',changed.status)
  self.assertEqual(before,self.snapshot())
  with db_backend.transaction(self.g) as c:
   c.execute('CREATE TABLE partner_breakthrough_operations(operation_id TEXT PRIMARY KEY,payload TEXT,reward_exp INTEGER,created_at TIMESTAMP)')
   c.execute("CREATE TRIGGER refuse_receipt BEFORE INSERT ON partner_breakthrough_operations BEGIN SELECT RAISE(ABORT,'receipt refused'); END")
  before=self.snapshot()
  with self.assertRaises(db_backend.IntegrityError):
   self.s.apply('refused','a','b','筑基',expected_user_exp=1000,expected_partner_exp=500,expected_affection=100000000000000000001,reward_exp=10,partner_power=99)
  self.assertEqual(before,self.snapshot())
