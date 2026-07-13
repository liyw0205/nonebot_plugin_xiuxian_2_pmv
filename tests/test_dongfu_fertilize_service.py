from __future__ import annotations
import json
import tempfile
import unittest
from pathlib import Path
import nonebot
nonebot.init()
from nonebot_plugin_xiuxian_2.xiuxian.xiuxian_dongfu.fertilize_service import DongfuFertilizeService
from tests.test_db_backend import db_backend

class DongfuFertilizeServiceTests(unittest.TestCase):
 def setUp(self):
  self.temp_dir=tempfile.TemporaryDirectory(); root=Path(self.temp_dir.name); self.game,self.player=root/"game.sqlite3",root/"player.sqlite3"; self.slots=[{"slot":1,"seed_id":21001,"fertilizer":0}]; self.expected=json.dumps(self.slots,ensure_ascii=False)
  with db_backend.transaction(self.game) as c: c.execute("CREATE TABLE back (user_id TEXT,goods_id INTEGER,goods_num INTEGER)"); c.execute("INSERT INTO back VALUES (%s,%s,%s)",("u",21006,2))
  with db_backend.transaction(self.player) as c: c.execute("CREATE TABLE dongfu_status (user_id TEXT PRIMARY KEY,built INTEGER,plant_slots TEXT)"); c.execute("INSERT INTO dongfu_status VALUES (%s,%s,%s)",("u",1,self.expected))
  self.service=DongfuFertilizeService(self.game,self.player)
 def tearDown(self): self.temp_dir.cleanup()
 def fertilize(self,op="op",**kw):
  v=dict(slots=self.expected,slot=1,item=21006,maximum=3);v.update(kw);return self.service.fertilize(op,"u",v["slots"],v["slot"],v["item"],v["maximum"])
 def state(self):
  with db_backend.connection(self.game) as c: item=c.execute("SELECT goods_num FROM back WHERE user_id=%s AND goods_id=%s",("u",21006)).fetchone()
  with db_backend.connection(self.player) as c: slots=c.execute("SELECT plant_slots FROM dongfu_status WHERE user_id=%s",("u",)).fetchone()
  return int(item[0]),int(json.loads(slots[0])[0]["fertilizer"])
 def test_success_and_duplicate(self): self.assertEqual(self.fertilize("same").status,"fertilized");self.assertEqual(self.fertilize("same").status,"duplicate");self.assertEqual(self.state(),(1,1))
 def test_rejections_preserve_state(self):
  self.assertEqual(self.fertilize("stale",slots="[]").status,"state_changed")
  self.assertEqual(self.fertilize("none",item=1).status,"item_insufficient")
  self.assertEqual(self.state(),(2,0))
 def test_failure_rolls_back(self):
  with db_backend.transaction(self.game) as c: c.execute("CREATE TABLE dongfu_fertilize_operations (operation_id TEXT PRIMARY KEY,payload TEXT NOT NULL,created_at TIMESTAMP)");c.execute("CREATE TRIGGER fail_fertilize BEFORE INSERT ON dongfu_fertilize_operations BEGIN SELECT RAISE(ABORT, 'failed'); END")
  with self.assertRaises(db_backend.IntegrityError): self.fertilize("rollback")
  self.assertEqual(self.state(),(2,0))
