from __future__ import annotations
import json
import tempfile
import unittest
from pathlib import Path
import nonebot
nonebot.init()
from nonebot_plugin_xiuxian_2.xiuxian.xiuxian_dongfu.transaction_service import InfiltrateSuccessService
from tests.test_db_backend import db_backend

class InfiltrateSuccessServiceTests(unittest.TestCase):
 def setUp(self):
  self.temp=tempfile.TemporaryDirectory();root=Path(self.temp.name);self.game,self.player=root/"g.db",root/"p.db";self.slots=[{"slot":1,"seed_id":21001,"plant_finish":"2026-07-13 12:00:00"}];self.expected=json.dumps(self.slots)
  with db_backend.transaction(self.game) as c:c.execute("CREATE TABLE user_xiuxian (user_id TEXT PRIMARY KEY,stone INTEGER)");c.execute("INSERT INTO user_xiuxian VALUES (%s,%s)",("u",100));c.execute("CREATE TABLE back (user_id TEXT,goods_id INTEGER,goods_name TEXT,goods_type TEXT,goods_num INTEGER,create_time TEXT,update_time TEXT,bind_num INTEGER,UNIQUE(user_id,goods_id))")
  with db_backend.transaction(self.player) as c:c.execute("CREATE TABLE dongfu_status (user_id TEXT PRIMARY KEY,built INTEGER,infiltrate_date TEXT,infiltrate_active_count INTEGER,infiltrate_random_count INTEGER,intrude_date TEXT,intrude_count INTEGER,patrol_guard INTEGER,plant_slots TEXT)");c.execute("INSERT INTO dongfu_status VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s)",("u",1,"",0,0,"",0,0,"[]"));c.execute("INSERT INTO dongfu_status VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s)",("t",1,"",0,0,"",0,1,self.expected))
  self.service=InfiltrateSuccessService(self.game,self.player)
 def tearDown(self):self.temp.cleanup()
 def settle(self,op="op",**kw):
  v=dict(slots=self.expected,finish="2026-07-13 13:00:00",rewards=((3001,"药材","药材",2),),stone=500,maximum=99);v.update(kw);return self.service.settle(op,"u","t","2026-07-13","infiltrate_active_count",3,3,v["slots"],1,v["finish"],v["rewards"],v["stone"],True,v["maximum"])
 def state(self):
  with db_backend.connection(self.game) as c:stone=int(c.execute("SELECT stone FROM user_xiuxian WHERE user_id=%s",("u",)).fetchone()[0]);item=c.execute("SELECT goods_num FROM back WHERE user_id=%s AND goods_id=%s",("u",3001)).fetchone()
  with db_backend.connection(self.player) as c:u=tuple(c.execute("SELECT infiltrate_active_count FROM dongfu_status WHERE user_id=%s",("u",)).fetchone());t=c.execute("SELECT intrude_count,patrol_guard,plant_slots FROM dongfu_status WHERE user_id=%s",("t",)).fetchone()
  return stone,int(item[0]) if item else 0,int(u[0]),int(t[0]),int(t[1]),json.loads(t[2])[0]["plant_finish"]
 def test_success_and_duplicate(self):self.assertEqual(self.settle("same").status,"settled");self.assertEqual(self.settle("same").status,"duplicate");self.assertEqual(self.state(),(600,2,1,1,0,"2026-07-13 13:00:00"))
 def test_capacity_and_snapshot_rejections(self):self.assertEqual(self.settle("full",maximum=1).status,"inventory_full");self.assertEqual(self.settle("stale",slots="[]").status,"state_changed");self.assertEqual(self.state(),(100,0,0,0,1,"2026-07-13 12:00:00"))
 def test_failure_rolls_back(self):
  with db_backend.transaction(self.game) as c:c.execute("CREATE TABLE dongfu_infiltrate_success_operations (operation_id TEXT PRIMARY KEY,payload TEXT NOT NULL,infiltrate_left INTEGER NOT NULL,intrude_left INTEGER NOT NULL,created_at TIMESTAMP)");c.execute("CREATE TRIGGER fail_success BEFORE INSERT ON dongfu_infiltrate_success_operations BEGIN SELECT RAISE(ABORT, 'failed'); END")
  with self.assertRaises(db_backend.IntegrityError):self.settle("bad")
  self.assertEqual(self.state(),(100,0,0,0,1,"2026-07-13 12:00:00"))
