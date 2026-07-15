from __future__ import annotations
import tempfile
import unittest
from pathlib import Path
import nonebot
nonebot.init()
from nonebot_plugin_xiuxian_2.xiuxian.xiuxian_dufang.transaction_service import DufangBetService, DufangPayoutService
from tests.test_db_backend import db_backend

class DufangPayoutServiceTests(unittest.TestCase):
 def setUp(self):
  self.temp=tempfile.TemporaryDirectory();root=Path(self.temp.name);self.game=root/'g.db';self.player=root/'p.db'
  with db_backend.transaction(self.game) as c:c.execute('CREATE TABLE user_xiuxian (user_id TEXT PRIMARY KEY,stone INTEGER)');c.execute('INSERT INTO user_xiuxian VALUES (%s,%s)',('u',1000))
  self.bet=DufangBetService(self.game,self.player);self.pay=DufangPayoutService(self.game,self.player);self.bet.place('bet','u',300,'start')
 def tearDown(self):self.temp.cleanup()
 def state(self):
  with db_backend.connection(self.game) as c:return int(c.execute('SELECT stone FROM user_xiuxian').fetchone()[0]),str(c.execute('SELECT status FROM dufang_bets').fetchone()[0])
 def test_win_and_duplicate(self):
  self.assertEqual(self.pay.settle('pay','bet','u','win',600,0,'end').status,'applied');self.assertEqual(self.pay.settle('pay','bet','u','win',600,0,'end').status,'duplicate');self.assertEqual(self.state(),(1300,'win'))
 def test_loss_is_bounded_and_repeated_bet_rejected(self):
  r=self.pay.settle('loss','bet','u','loss',0,900,'end');self.assertEqual((r.loss,self.state()),(700,(0,'loss')));self.assertEqual(self.pay.settle('again','bet','u','loss',0,1,'later').status,'state_changed')
 def test_record_failure_rolls_back(self):
  with db_backend.transaction(self.game) as c:c.execute('CREATE TABLE dufang_payout_operations (operation_id TEXT PRIMARY KEY,payload TEXT,wallet_stone INTEGER,gain INTEGER,loss INTEGER,created_at TIMESTAMP)');c.execute("CREATE TRIGGER fail_pay BEFORE INSERT ON dufang_payout_operations BEGIN SELECT RAISE(ABORT,'failed'); END")
  with self.assertRaises(db_backend.IntegrityError):self.pay.settle('bad','bet','u','win',100,0,'end')
  self.assertEqual(self.state(),(700,'pending'))
