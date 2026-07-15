from __future__ import annotations
import tempfile
import unittest
from pathlib import Path
import nonebot
nonebot.init()
from nonebot_plugin_xiuxian_2.xiuxian.xiuxian_impart_pk.closing_settlement_service import ImpartClosingSettlementService
from tests.test_db_backend import db_backend

class ImpartClosingSettlementTests(unittest.TestCase):
    def setUp(self):
        self.tmp=tempfile.TemporaryDirectory(); root=Path(self.tmp.name)
        self.game,self.impart,self.player=root/"game.db",root/"impart.db",root/"player.db"
        with db_backend.transaction(self.game) as c:
            c.execute("CREATE TABLE user_xiuxian(user_id TEXT PRIMARY KEY,exp INTEGER,hp INTEGER,mp INTEGER,atk INTEGER,power INTEGER)")
            c.execute("CREATE TABLE user_cd(user_id TEXT PRIMARY KEY,type INTEGER,create_time TEXT,scheduled_time TEXT)")
            c.execute("INSERT INTO user_xiuxian VALUES(%s,%s,%s,%s,%s,%s)",( "u",100,1,2,3,4)); c.execute("INSERT INTO user_cd VALUES(%s,%s,%s,%s)",( "u",4,"start",None))
        with db_backend.transaction(self.impart) as c:
            c.execute("CREATE TABLE xiuxian_impart(user_id TEXT PRIMARY KEY,exp_day INTEGER)"); c.execute("INSERT INTO xiuxian_impart VALUES(%s,%s)",( "u",30))
        self.service=ImpartClosingSettlementService(self.game,self.impart,self.player)
    def tearDown(self): self.tmp.cleanup()
    def state(self):
        with db_backend.connection(self.game) as c: user=tuple(c.execute("SELECT exp,hp,mp,atk,power FROM user_xiuxian").fetchone()); cd=tuple(c.execute("SELECT type,create_time FROM user_cd").fetchone())
        with db_backend.connection(self.impart) as c: day=c.execute("SELECT exp_day FROM xiuxian_impart").fetchone()[0]
        return user,cd,day
    def test_atomic_idempotent_settlement(self):
        args=("op","u","start",100,30,20,10,60,40,50,6,999)
        self.assertEqual("applied",self.service.settle(*args).status)
        alt=("op","u","start",100,30,999,10,60,40,50,6,999)
        self.assertEqual("duplicate",self.service.settle(*alt).status)
        self.assertIsNotNone(self.service.get_result("op"))
        self.assertEqual(((120,40,50,6,999),(0,"0"),20),self.state())
        with db_backend.connection(self.player) as c: stats=tuple(c.execute('SELECT "虚神界闭关时长","虚神界闭关修为","虚神界闭关祝福时长" FROM statistics').fetchone())
        self.assertEqual((60,20,10),stats)
    def test_stale_and_failure_roll_back(self):
        before=self.state(); self.assertEqual("state_changed",self.service.settle("stale","u","bad",100,30,20,10,60,40,50,6,999).status); self.assertEqual(before,self.state())
        with db_backend.transaction(self.game) as c:
            c.execute("CREATE TABLE impart_closing_operations(operation_id TEXT PRIMARY KEY,payload TEXT,result_json TEXT)"); c.execute("CREATE TRIGGER reject_impart_close BEFORE INSERT ON impart_closing_operations BEGIN SELECT RAISE(ABORT,'reject'); END")
        with self.assertRaises(db_backend.IntegrityError): self.service.settle("fail","u","start",100,30,20,10,60,40,50,6,999)
        self.assertEqual(before,self.state())
if __name__ == "__main__": unittest.main()
