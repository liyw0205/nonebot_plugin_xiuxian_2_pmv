import tempfile
import unittest
from pathlib import Path
import nonebot

nonebot.init()

from nonebot_plugin_xiuxian_2.xiuxian.xiuxian_impart_pk.explore_settlement_service import ImpartExploreSettlementService
from tests.test_db_backend import db_backend


class ImpartExploreSettlementTests(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory(); root = Path(self.tmp.name)
        self.game, self.impart, self.player = root / "game.db", root / "impart.db", root / "player.db"
        with db_backend.transaction(self.impart) as conn:
            conn.execute("CREATE TABLE xiuxian_impart(user_id TEXT PRIMARY KEY,exp_day INTEGER,impart_lv INTEGER)")
            conn.execute("INSERT INTO xiuxian_impart VALUES('u',300,4)")
        self.service = ImpartExploreSettlementService(self.game, self.impart, self.player)
        self.legacy = {"pk_num": 7, "impart_num": 10, "exp_used": 0, "exp_count": 0, "exp_load": 0, "exp_gain": 0}

    def tearDown(self): self.tmp.cleanup()

    def call(self, operation="explore", expected_num=10):
        return self.service.settle(operation, "u", event_type="up", expected_exp_day=300,
            expected_impart_lv=4, expected_impart_num=expected_num, time_cost=50,
            new_impart_lv=5, legacy_state=self.legacy)

    def test_atomic_event_and_idempotency(self):
        self.assertEqual("applied", self.call().status)
        # mutable expected_impart_num must not break same-op replay
        self.assertEqual("duplicate", self.call(expected_num=9).status)
        self.assertIsNotNone(self.service.get_result("explore"))
        with db_backend.connection(self.impart) as conn: self.assertEqual((250, 5), tuple(conn.execute("SELECT exp_day,impart_lv FROM xiuxian_impart").fetchone()))
        with db_backend.connection(self.player) as conn:
            self.assertEqual(9, conn.execute("SELECT impart_num FROM impart_pk_daily").fetchone()[0])
            self.assertEqual((1, 50, 1), tuple(conn.execute('SELECT "虚神界探索次数","虚神界探索消耗时间","虚神界探索上升" FROM statistics').fetchone()))

    def test_snapshot_and_rollback(self):
        self.assertEqual("state_changed", self.call(expected_num=9).status)
        with db_backend.transaction(self.game) as conn:
            conn.execute("CREATE TABLE impart_explore_operations(operation_id TEXT PRIMARY KEY,payload TEXT,result_json TEXT)")
            conn.execute("CREATE TRIGGER fail_explore BEFORE INSERT ON impart_explore_operations BEGIN SELECT RAISE(ABORT,'x'); END")
        with self.assertRaises(Exception): self.call("rollback")
        with db_backend.connection(self.impart) as conn: self.assertEqual((300, 4), tuple(conn.execute("SELECT exp_day,impart_lv FROM xiuxian_impart").fetchone()))
