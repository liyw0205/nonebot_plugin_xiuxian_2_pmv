from __future__ import annotations
import tempfile
import unittest
from pathlib import Path
import nonebot
nonebot.init()
from nonebot_plugin_xiuxian_2.xiuxian.xiuxian_buff.transaction_service import ClosingSettlementService
from tests.test_db_backend import db_backend

class ClosingSettlementServiceTests(unittest.TestCase):
    def setUp(self):
        self.tmp=tempfile.TemporaryDirectory(); self.db=Path(self.tmp.name)/"game.db"
        with db_backend.transaction(self.db) as c:
            c.execute("CREATE TABLE user_xiuxian (user_id TEXT PRIMARY KEY,exp INTEGER,stone INTEGER,hp INTEGER,mp INTEGER,atk INTEGER,power INTEGER)")
            c.execute("CREATE TABLE user_cd (user_id TEXT PRIMARY KEY,type INTEGER,create_time TEXT,scheduled_time TEXT)")
            c.execute("INSERT INTO user_xiuxian VALUES (%s,%s,%s,%s,%s,%s,%s)",("u",100,50,1,2,3,4))
            c.execute("INSERT INTO user_cd VALUES (%s,%s,%s,%s)",("u",1,"start",None))
        self.service=ClosingSettlementService(self.db)
    def tearDown(self): self.tmp.cleanup()
    def state(self):
        with db_backend.connection(self.db) as c:
            return tuple(c.execute("SELECT exp,stone,hp,mp,atk,power FROM user_xiuxian WHERE user_id=%s",("u",)).fetchone()),tuple(c.execute("SELECT type,create_time FROM user_cd WHERE user_id=%s",("u",)).fetchone())
    def test_settlement_is_atomic_and_idempotent(self):
        first=self.service.settle("op","u","start",20,10,30,40,5,999); second=self.service.settle("op","u","start",20,10,30,40,5,999)
        self.assertEqual("applied",first.status); self.assertEqual("duplicate",second.status)
        self.assertEqual(((120,40,30,40,5,999),(0,"0")),self.state())
    def test_stale_or_failed_operation_rolls_back(self):
        before=self.state(); self.assertEqual("state_changed",self.service.settle("stale","u","other",20,0,1,1,1,1).status); self.assertEqual(before,self.state())
        with db_backend.transaction(self.db) as c:
            c.execute("CREATE TABLE closing_settlement_operations (operation_id TEXT PRIMARY KEY,payload TEXT,result_json TEXT)")
            c.execute("CREATE TRIGGER reject_close BEFORE INSERT ON closing_settlement_operations BEGIN SELECT RAISE(ABORT,'reject'); END")
        with self.assertRaises(db_backend.IntegrityError): self.service.settle("fail","u","start",20,0,1,1,1,1)
        self.assertEqual(before,self.state())
if __name__ == "__main__": unittest.main()
