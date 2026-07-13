from __future__ import annotations
import tempfile
import unittest
from pathlib import Path
import nonebot
nonebot.init()
from nonebot_plugin_xiuxian_2.xiuxian.xiuxian_dongfu.array_upgrade_service import DongfuArrayUpgradeService
from tests.test_db_backend import db_backend

class DongfuArrayUpgradeServiceTests(unittest.TestCase):
 def setUp(self):
  self.temp_dir=tempfile.TemporaryDirectory(); root=Path(self.temp_dir.name); self.game,self.player=root/"game.sqlite3",root/"player.sqlite3"
  with db_backend.transaction(self.game) as c:
   c.execute("CREATE TABLE user_xiuxian (user_id TEXT PRIMARY KEY,stone INTEGER)"); c.execute("INSERT INTO user_xiuxian VALUES (%s,%s)",("u",1000)); c.execute("CREATE TABLE back (user_id TEXT,goods_id INTEGER,goods_num INTEGER)"); c.execute("INSERT INTO back VALUES (%s,%s,%s)",("u",21007,2))
  with db_backend.transaction(self.player) as c:
   c.execute("CREATE TABLE dongfu_status (user_id TEXT PRIMARY KEY,built INTEGER,array_level INTEGER)"); c.execute("INSERT INTO dongfu_status VALUES (%s,%s,%s)",("u",1,3))
  self.service=DongfuArrayUpgradeService(self.game,self.player)
 def tearDown(self): self.temp_dir.cleanup()
 def upgrade(self,op="op",**kw):
  v=dict(level=3,next=4,stone=500,item=21007,amount=1); v.update(kw); return self.service.upgrade(op,"u",v["level"],v["next"],v["stone"],v["item"],v["amount"])
 def state(self):
  with db_backend.connection(self.game) as c: u=c.execute("SELECT stone FROM user_xiuxian WHERE user_id=%s",("u",)).fetchone(); i=c.execute("SELECT goods_num FROM back WHERE user_id=%s AND goods_id=%s",("u",21007)).fetchone()
  with db_backend.connection(self.player) as c: d=c.execute("SELECT array_level FROM dongfu_status WHERE user_id=%s",("u",)).fetchone()
  return int(u[0]),int(i[0]),int(d[0])
 def test_success_and_duplicate(self):
  self.assertEqual(self.upgrade("same").status,"upgraded"); self.assertEqual(self.upgrade("same").status,"duplicate"); self.assertEqual(self.state(),(500,1,4))
 def test_rejections_leave_state(self):
  self.assertEqual(self.upgrade("stone",stone=1001).status,"stone_insufficient"); self.assertEqual(self.upgrade("item",amount=3).status,"item_insufficient"); self.assertEqual(self.upgrade("stale",level=2,next=3).status,"state_changed"); self.assertEqual(self.state(),(1000,2,3))
 def test_failure_rolls_back(self):
  with db_backend.transaction(self.game) as c: c.execute("CREATE TABLE dongfu_array_upgrade_operations (operation_id TEXT PRIMARY KEY,payload TEXT NOT NULL,level INTEGER NOT NULL,created_at TIMESTAMP)"); c.execute("CREATE TRIGGER fail_array BEFORE INSERT ON dongfu_array_upgrade_operations BEGIN SELECT RAISE(ABORT, 'failed'); END")
  with self.assertRaises(db_backend.IntegrityError): self.upgrade("rollback")
  self.assertEqual(self.state(),(1000,2,3))
