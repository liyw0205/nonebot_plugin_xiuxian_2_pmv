import tempfile
import unittest
from pathlib import Path

import nonebot
nonebot.init()

from nonebot_plugin_xiuxian_2.xiuxian.xiuxian_lunhui.settlement_service import LunhuiSettlementService
from tests.test_db_backend import db_backend


class LunhuiSettlementServiceTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        root = Path(self.temp.name)
        self.game, self.player, self.impart = root / "game.db", root / "player.db", root / "impart.db"
        with db_backend.transaction(self.game) as conn:
            conn.execute("CREATE TABLE user_xiuxian(user_id TEXT PRIMARY KEY,user_name TEXT,level TEXT,exp INTEGER,stone INTEGER,level_up_rate INTEGER,root TEXT,root_type TEXT,root_level INTEGER,power INTEGER,hp INTEGER,mp INTEGER,atk INTEGER,atkpractice INTEGER,hppractice INTEGER,mppractice INTEGER)")
            conn.execute("INSERT INTO user_xiuxian VALUES('u','道友','渡劫',999,200000000,5,'旧根','旧',1,9,8,7,6,5,4,3)")
            conn.execute("CREATE TABLE BuffInfo(user_id TEXT PRIMARY KEY,main_buff INTEGER,sub_buff INTEGER,sec_buff INTEGER,effect1_buff INTEGER,effect2_buff INTEGER)")
            conn.execute("INSERT INTO BuffInfo VALUES('u',1,2,3,4,5)")
            conn.execute("CREATE TABLE back(user_id TEXT,goods_id INTEGER,goods_name TEXT,goods_type TEXT,goods_num INTEGER,bind_num INTEGER,all_num INTEGER,UNIQUE(user_id,goods_id))")
            conn.execute("INSERT INTO back VALUES('u',10,'丹','丹药',1,0,9)")
        with db_backend.transaction(self.impart) as conn:
            conn.execute("CREATE TABLE xiuxian_impart(user_id TEXT PRIMARY KEY,exp_day INTEGER,stone_num INTEGER)")
            conn.execute("INSERT INTO xiuxian_impart VALUES('u',12,250)")
        self.service = LunhuiSettlementService(self.game, self.player, self.impart)
        self.buffs = {"main_buff": 1, "sub_buff": 2, "sec_buff": 3, "effect1_buff": 4, "effect2_buff": 5}

    def tearDown(self):
        self.temp.cleanup()

    def call(self, operation="op", root_key=9, stone=200000000):
        return self.service.settle(operation, "u", "渡劫", root_key, "旧", 20025, "灵根改名卡", expected_exp=999, expected_stone=stone, expected_root_level=1, expected_buffs=self.buffs, expected_impart_exp_day=12, expected_impart_stone=250, user_name="道友")

    def test_comprehensive_atomic_settlement_and_idempotency(self):
        result = self.call()
        self.assertEqual(("applied", 100000000, 2, 2), (result.status, result.stone, result.root_level, result.wishing_stones))
        # mutable expected_stone must not break same-op replay
        self.assertEqual("duplicate", self.call(stone=1).status)
        # different root_key is different request identity
        self.assertEqual("operation_conflict", self.call(root_key=8).status)
        self.assertIsNotNone(self.service.get_result("op"))
        with db_backend.connection(self.game) as conn:
            user = conn.execute("SELECT level,exp,stone,root_type,root_level,hp,mp,atk FROM user_xiuxian").fetchone()
            items = conn.execute("SELECT goods_id,goods_num FROM back WHERE goods_type='特殊道具' ORDER BY goods_id").fetchall()
            self.assertEqual(0, conn.execute("SELECT all_num FROM back WHERE goods_id=10").fetchone()[0])
        self.assertEqual(("江湖好手", 100, 100000000, "命运道果", 2, 50, 100, 10), tuple(user))
        self.assertEqual([(20005, 2), (20025, 1)], [tuple(row) for row in items])
        with db_backend.connection(self.impart) as conn:
            self.assertEqual((0, 0), tuple(conn.execute("SELECT exp_day,stone_num FROM xiuxian_impart").fetchone()))

    def test_snapshot_change_and_cross_database_rollback(self):
        self.assertEqual("state_changed", self.call("stale", stone=1).status)
        with db_backend.transaction(self.game) as conn:
            conn.execute("CREATE TABLE IF NOT EXISTS lunhui_settlement_operations(operation_id TEXT PRIMARY KEY,payload TEXT,stone INTEGER,root_level INTEGER,wishing_stones INTEGER)")
            conn.execute("CREATE TRIGGER fail_lunhui BEFORE INSERT ON lunhui_settlement_operations BEGIN SELECT RAISE(ABORT,'fail'); END")
        with self.assertRaises(Exception):
            self.call("fail")
        with db_backend.connection(self.game) as conn:
            self.assertEqual(("渡劫", 999, 200000000), tuple(conn.execute("SELECT level,exp,stone FROM user_xiuxian").fetchone()))
            self.assertIsNone(conn.execute("SELECT 1 FROM back WHERE goods_id=20025").fetchone())
        with db_backend.connection(self.impart) as conn:
            self.assertEqual((12, 250), tuple(conn.execute("SELECT exp_day,stone_num FROM xiuxian_impart").fetchone()))
