import json
import tempfile
import unittest
from pathlib import Path
import nonebot
nonebot.init()
from nonebot_plugin_xiuxian_2.xiuxian.xiuxian_buff.transaction_service import MentorTransmissionService
from tests.test_db_backend import db_backend

class Tests(unittest.TestCase):
 def setUp(self):
  self.t=tempfile.TemporaryDirectory();r=Path(self.t.name);self.g=r/'g';self.p=r/'p'
  with db_backend.transaction(self.g) as c:
   c.execute('CREATE TABLE user_xiuxian(user_id TEXT PRIMARY KEY,exp INTEGER,power INTEGER,hp INTEGER,mp INTEGER,atk INTEGER)');c.executemany('INSERT INTO user_xiuxian VALUES (%s,%s,0,1,1,1)',[('m',5000),('a',1000)])
  with db_backend.transaction(self.p) as c:
   c.execute('CREATE TABLE mentor(user_id TEXT PRIMARY KEY,mentor_id TEXT,apprentice_ids TEXT,mentor_history TEXT)');c.executemany('INSERT INTO mentor VALUES (%s,%s,%s,%s)',[('m',None,json.dumps(['a']),'[]'),('a','m','[]','[]')])
  self.s=MentorTransmissionService(self.g,self.p)
 def tearDown(self):self.t.cleanup()
 def call(self,op='x',reward=100):return self.s.apply(op,'m','a',expected_apprentice_exp=1000,reward_exp=reward,power=9,hp=10,mp=11,atk=12,mentor_used=0,apprentice_used=0,daily_limit=3,history_limit=50,mentor_desc='give',apprentice_desc='receive')
 def test_atomic_idempotency_conflict(self):
  self.assertEqual('applied',self.call().status);self.assertEqual('duplicate',self.call().status);self.assertEqual('operation_conflict',self.call(reward=101).status)
  with db_backend.connection(self.g) as c:self.assertEqual((1100,9,10,11,12),tuple(c.execute("SELECT exp,power,hp,mp,atk FROM user_xiuxian WHERE user_id='a'").fetchone()))
  with db_backend.connection(self.p) as c:
   self.assertEqual(1,c.execute('SELECT "师徒传功次数" FROM statistics WHERE user_id=\'m\'').fetchone()[0]);self.assertEqual((1,100),tuple(c.execute('SELECT "接受传功次数","传功获得修为" FROM statistics WHERE user_id=\'a\'').fetchone()))
 def test_snapshot_and_rollback(self):
  with db_backend.transaction(self.g) as c:c.execute("UPDATE user_xiuxian SET exp=999 WHERE user_id='a'")
  self.assertEqual('state_changed',self.call().status)
  with db_backend.transaction(self.g) as c:c.execute("UPDATE user_xiuxian SET exp=1000 WHERE user_id='a'")
  self.call('seed')
  with db_backend.transaction(self.g) as c:c.execute("CREATE TRIGGER fail BEFORE INSERT ON mentor_transmission_operations BEGIN SELECT RAISE(ABORT,'x'); END");c.execute("UPDATE user_xiuxian SET exp=1000,power=0,hp=1,mp=1,atk=1 WHERE user_id='a'")
  with self.assertRaises(Exception):self.call('fail')
  with db_backend.connection(self.g) as c:self.assertEqual(1000,c.execute("SELECT exp FROM user_xiuxian WHERE user_id='a'").fetchone()[0])
