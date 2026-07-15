from __future__ import annotations
import tempfile
import unittest
from pathlib import Path
import nonebot
nonebot.init()
from nonebot_plugin_xiuxian_2.xiuxian.xiuxian_dongfu.transaction_service import DongfuVisitRewardService
from tests.test_db_backend import db_backend

class DongfuVisitRewardServiceTests(unittest.TestCase):
 def setUp(self):
  self.temp_dir=tempfile.TemporaryDirectory(); root=Path(self.temp_dir.name); self.game,self.player=root/"game.sqlite3",root/"player.sqlite3"
  with db_backend.transaction(self.game) as c: c.execute("CREATE TABLE user_xiuxian (user_id TEXT PRIMARY KEY,stone INTEGER)"); c.execute("INSERT INTO user_xiuxian VALUES (%s,%s)",("u",100))
  with db_backend.transaction(self.player) as c: c.execute("CREATE TABLE dongfu_status (user_id TEXT PRIMARY KEY,built INTEGER)"); c.execute("INSERT INTO dongfu_status VALUES (%s,%s)",("u",1)); c.execute("INSERT INTO dongfu_status VALUES (%s,%s)",("t",1))
  self.service=DongfuVisitRewardService(self.game,self.player)
 def tearDown(self): self.temp_dir.cleanup()
 def reward(self,op="op",**kw):
  v=dict(target="t",gain=200); v.update(kw); return self.service.reward(op,"u",v["target"],v["gain"])
 def stone(self):
  with db_backend.connection(self.game) as c: return int(c.execute("SELECT stone FROM user_xiuxian WHERE user_id=%s",("u",)).fetchone()[0])
 def test_success_and_duplicate(self):
  self.assertEqual(self.reward("same").status,"rewarded"); self.assertEqual(self.reward("same").status,"duplicate"); self.assertEqual(self.stone(),300)
 def test_target_change_does_not_reward(self):
  with db_backend.transaction(self.player) as c: c.execute("UPDATE dongfu_status SET built=0 WHERE user_id=%s",("t",))
  self.assertEqual(self.reward("changed").status,"dongfu_changed"); self.assertEqual(self.stone(),100)
 def test_failure_rolls_back(self):
  with db_backend.transaction(self.game) as c: c.execute("CREATE TABLE dongfu_visit_reward_operations (operation_id TEXT PRIMARY KEY,payload TEXT NOT NULL,created_at TIMESTAMP)"); c.execute("CREATE TRIGGER fail_visit BEFORE INSERT ON dongfu_visit_reward_operations BEGIN SELECT RAISE(ABORT, 'failed'); END")
  with self.assertRaises(db_backend.IntegrityError): self.reward("rollback")
  self.assertEqual(self.stone(),100)
