import tempfile, unittest
from pathlib import Path
import nonebot

nonebot.init()
from nonebot_plugin_xiuxian_2.xiuxian.xiuxian_pet.transaction_service import PetHatchService
from tests.test_db_backend import db_backend

class T(unittest.TestCase):
 def setUp(self):
  self.t=tempfile.TemporaryDirectory(); r=Path(self.t.name); self.g=r/'g'; self.p=r/'p'
  with db_backend.transaction(self.g) as c: c.execute('CREATE TABLE user_xiuxian(user_id TEXT PRIMARY KEY,stone INTEGER)'); c.execute("INSERT INTO user_xiuxian VALUES('u',100)")
  with db_backend.transaction(self.p) as c: c.execute('CREATE TABLE player_pet(user_id TEXT PRIMARY KEY,active_uid TEXT,egg_pity_count INTEGER,egg_pity_no_mythic_count INTEGER,travel TEXT)'); c.execute("INSERT INTO player_pet VALUES('u','',0,0,NULL)"); c.execute('CREATE TABLE player_pet_item(id TEXT,user_id TEXT,uid TEXT,is_active INTEGER,pet_id TEXT,stars INTEGER,exp INTEGER,total_exp INTEGER,skill_id TEXT,created_at INTEGER,updated_at INTEGER)')
  self.s=PetHatchService(self.g,self.p); self.pet={'uid':'x','pet_id':'1','stars':1,'exp':0,'total_exp':0,'skill':{}}
 def tearDown(self): self.t.cleanup()
 def call(self,op='o',stone=100,pet=None):
  pet=pet or self.pet
  return self.s.hatch(op,'u',stone,10,['',0,0,None],[(pet,True)],['x',1,0],10)
 def test_idempotent(self):
  first=self.call()
  self.assertEqual(first.status,'applied')
  # random pet blob / stone snapshot must not break same-op replay
  other={'uid':'y','pet_id':'2','stars':1,'exp':0,'total_exp':0,'skill':{}}
  second=self.call(stone=90,pet=other)
  self.assertEqual(second.status,'duplicate')
  self.assertEqual(second.pets[0][0]['uid'],'x')
  prior=self.s.get_result('o')
  self.assertIsNotNone(prior)
  self.assertEqual(prior.status,'duplicate')
 def test_snapshot(self): self.assertEqual(self.call(op='s',stone=99).status,'state_changed')
 def test_identity_conflict(self):
  self.assertEqual(self.call(op='c').status,'applied')
  # different request identity (cost/count) on same op id
  r=self.s.hatch('c','u',100,20,['',0,0,None],[(self.pet,True),({**self.pet,'uid':'z'},False)],['x',1,0],10)
  self.assertEqual(r.status,'state_changed')
 def test_rollback(self):
  with db_backend.transaction(self.g) as c: c.execute('CREATE TABLE pet_hatch_operations(operation_id TEXT PRIMARY KEY,payload TEXT,created_at TEXT)'); c.execute("CREATE TRIGGER f BEFORE INSERT ON pet_hatch_operations BEGIN SELECT RAISE(ABORT,'x');END")
  with self.assertRaises(Exception): self.call()
  with db_backend.connection(self.g) as c: self.assertEqual(c.execute('SELECT stone FROM user_xiuxian').fetchone()[0],100)
