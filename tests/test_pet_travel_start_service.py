import json, tempfile, unittest
from pathlib import Path
import nonebot

nonebot.init()
from nonebot_plugin_xiuxian_2.xiuxian.xiuxian_pet.transaction_service import PetTravelStartService
from tests.test_db_backend import db_backend

class T(unittest.TestCase):
 def setUp(self):
  self.t=tempfile.TemporaryDirectory(); self.db=Path(self.t.name)/'p'
  with db_backend.transaction(self.db) as c:
   c.execute('CREATE TABLE player_pet(user_id TEXT PRIMARY KEY,travel TEXT)'); c.execute("INSERT INTO player_pet VALUES('u',NULL)")
   c.execute('CREATE TABLE player_pet_item(user_id TEXT,uid TEXT,is_active INTEGER)'); c.execute("INSERT INTO player_pet_item VALUES('u','p',1)")
  self.s=PetTravelStartService(self.db); self.travel={'pet_uid':'p','start_at':1}
 def tearDown(self): self.t.cleanup()
 def test_idempotent(self):
  self.assertEqual(self.s.start('o','u','p',None,self.travel).status,'applied'); self.assertEqual(self.s.start('o','u','p',None,self.travel).status,'duplicate')
 def test_snapshot(self): self.assertEqual(self.s.start('o','u','p',{'x':1},self.travel).status,'state_changed')
 def test_rollback(self):
  with db_backend.transaction(self.db) as c: c.execute('CREATE TABLE pet_travel_start_operations(operation_id TEXT PRIMARY KEY,payload TEXT,created_at TEXT)'); c.execute("CREATE TRIGGER f BEFORE INSERT ON pet_travel_start_operations BEGIN SELECT RAISE(ABORT,'x');END")
  with self.assertRaises(Exception): self.s.start('o','u','p',None,self.travel)
  with db_backend.connection(self.db) as c: self.assertIsNone(c.execute('SELECT travel FROM player_pet').fetchone()[0])
