from __future__ import annotations

import tempfile
import unittest
from pathlib import Path

import nonebot

nonebot.init()

from nonebot_plugin_xiuxian_2.xiuxian.xiuxian_buff.stone_training_settlement_service import StoneTrainingSettlementService
from tests.test_db_backend import db_backend


class StoneTrainingSettlementTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        root = Path(self.temp.name)
        self.game, self.player = root / "game.db", root / "player.db"
        with db_backend.transaction(self.game) as conn:
            conn.execute("CREATE TABLE user_xiuxian(user_id TEXT PRIMARY KEY,exp INTEGER,stone INTEGER,power INTEGER)")
            conn.execute("INSERT INTO user_xiuxian VALUES('u',100,1000,1)")
        self.service = StoneTrainingSettlementService(self.game, self.player)

    def tearDown(self):
        self.temp.cleanup()

    def call(self, operation="op", requested=500, expected_exp=100, expected_stone=1000, cap=130):
        return self.service.settle(operation, "u", requested_stone=requested, expected_exp=expected_exp,
                                   expected_stone=expected_stone, exp_cap=cap, power_multiplier=2)

    def test_cap_cost_statistics_and_idempotency(self):
        first, second = self.call(), self.call()
        self.assertEqual(("applied", 30, 300, 260), (first.status, first.exp_gain, first.stone_cost, first.power))
        self.assertEqual("duplicate", second.status)
        with db_backend.connection(self.game) as conn:
            self.assertEqual((130, 700, 260), tuple(conn.execute("SELECT exp,stone,power FROM user_xiuxian").fetchone()))
        with db_backend.connection(self.player) as conn:
            self.assertEqual((300, 30), tuple(conn.execute('SELECT "灵石修炼","灵石修炼修为" FROM statistics').fetchone()))

    def test_rechecks_snapshot_balance_and_cap(self):
        self.assertEqual("state_changed", self.call("stale", expected_stone=999).status)
        self.assertEqual("stone_insufficient", self.call("poor", requested=2000, expected_stone=1000, cap=300).status)
        self.assertEqual("exp_capped", self.call("cap", cap=100).status)
        with db_backend.connection(self.game) as conn:
            self.assertEqual((100, 1000, 1), tuple(conn.execute("SELECT exp,stone,power FROM user_xiuxian").fetchone()))

    def test_failure_rolls_back_assets_and_statistics(self):
        with db_backend.transaction(self.game) as conn:
            conn.execute("CREATE TABLE stone_training_operations(operation_id TEXT PRIMARY KEY,payload TEXT,result_json TEXT)")
            conn.execute("CREATE TRIGGER fail_stone_training BEFORE INSERT ON stone_training_operations BEGIN SELECT RAISE(ABORT,'fail'); END")
        with self.assertRaises(Exception):
            self.call("fail")
        with db_backend.connection(self.game) as conn:
            self.assertEqual((100, 1000, 1), tuple(conn.execute("SELECT exp,stone,power FROM user_xiuxian").fetchone()))
        with db_backend.connection(self.player) as conn:
            self.assertIsNone(conn.execute("SELECT name FROM sqlite_master WHERE type='table' AND name='statistics'").fetchone())


if __name__ == "__main__":
    unittest.main()
