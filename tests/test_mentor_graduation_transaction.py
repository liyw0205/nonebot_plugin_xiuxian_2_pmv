import json,tempfile,unittest
from pathlib import Path
import nonebot
nonebot.init()
from nonebot_plugin_xiuxian_2.xiuxian.xiuxian_buff.transaction_service import MentorGraduationService
from tests.test_db_backend import db_backend
class Tests(unittest.TestCase):
 def setUp(self):
  self.t=tempfile.TemporaryDirectory();r=Path(self.t.name);self.g=r/'g';self.p=r/'p'
  with db_backend.transaction(self.g) as c:c.execute('CREATE TABLE user_xiuxian(user_id TEXT PRIMARY KEY,stone INTEGER)');c.executemany('INSERT INTO user_xiuxian VALUES (%s,%s)',[('m',10),('a',20)])
  with db_backend.transaction(self.p) as c:c.execute('CREATE TABLE mentor(user_id TEXT PRIMARY KEY,mentor_id TEXT,apprentice_ids TEXT,mentor_rebind_cd TEXT,mentor_history TEXT,bind_time TEXT,breakthrough_reward_count INTEGER)');c.executemany('INSERT INTO mentor VALUES (%s,%s,%s,%s,%s,%s,%s)',[('m',None,json.dumps(['a']),'{}','[]',None,0),('a','m','[]','{}','[]','now',2)])
  self.s=MentorGraduationService(self.g,self.p)
 def tearDown(self):self.t.cleanup()
 def call(self,op='x',ar=5):return self.s.apply(op,'m','a',expected_mentor_stone=10,expected_apprentice_stone=20,apprentice_reward=ar,mentor_reward=7,cooldown_days=7,history_limit=50,mentor_desc='graduate',apprentice_desc='graduate',apprentice_title_ids=['1'],mentor_title_ids=['2'])
 def test_atomic_idempotency_conflict(self):
  self.assertEqual('applied',self.call().status);self.assertEqual('duplicate',self.call().status);self.assertEqual('operation_conflict',self.call(ar=6).status)
  with db_backend.connection(self.g) as c:self.assertEqual([25,17],[r[0] for r in c.execute('SELECT stone FROM user_xiuxian ORDER BY user_id').fetchall()])
  with db_backend.connection(self.p) as c:self.assertIsNone(c.execute("SELECT mentor_id FROM mentor WHERE user_id='a'").fetchone()[0])
 def test_snapshot_and_rollback(self):
  with db_backend.transaction(self.g) as c:c.execute("UPDATE user_xiuxian SET stone=21 WHERE user_id='a'")
  self.assertEqual('state_changed',self.call().status)
  with db_backend.transaction(self.g) as c:c.execute("UPDATE user_xiuxian SET stone=20 WHERE user_id='a'")
  self.call('seed')
  with db_backend.transaction(self.g) as c:c.execute("CREATE TRIGGER fail BEFORE INSERT ON mentor_graduation_operations BEGIN SELECT RAISE(ABORT,'x'); END");c.execute("UPDATE user_xiuxian SET stone=10 WHERE user_id='m'");c.execute("UPDATE user_xiuxian SET stone=20 WHERE user_id='a'")
  with db_backend.transaction(self.p) as c:c.execute("UPDATE mentor SET apprentice_ids='[\"a\"]' WHERE user_id='m'");c.execute("UPDATE mentor SET mentor_id='m' WHERE user_id='a'")
  with self.assertRaises(Exception):self.call('fail')
  with db_backend.connection(self.g) as c:self.assertEqual(20,c.execute("SELECT stone FROM user_xiuxian WHERE user_id='a'").fetchone()[0])
