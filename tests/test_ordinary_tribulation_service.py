import tempfile
import unittest
from pathlib import Path

import nonebot

nonebot.init()

from nonebot_plugin_xiuxian_2.xiuxian.xiuxian_base.transaction_service import OrdinaryTribulationService
from tests.test_db_backend import db_backend


class OrdinaryTribulationServiceTests(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory(); root = Path(self.tmp.name)
        self.game, self.player = root / "game.db", root / "player.db"
        with db_backend.transaction(self.game) as conn:
            conn.execute("CREATE TABLE user_xiuxian(user_id TEXT PRIMARY KEY,level TEXT,exp INTEGER,power INTEGER)")
            conn.execute("CREATE TABLE user_tribulation(user_id TEXT PRIMARY KEY,current_rate INTEGER,heart_devil_count INTEGER,last_time TEXT)")
            conn.execute("CREATE TABLE back(user_id TEXT,goods_id INTEGER,goods_num INTEGER,bind_num INTEGER,day_num INTEGER,all_num INTEGER,update_time TEXT,action_time TEXT,PRIMARY KEY(user_id,goods_id))")
            conn.execute("INSERT INTO user_xiuxian VALUES('u','元婴境圆满',1000,10)")
            conn.execute("INSERT INTO user_tribulation VALUES('u',40,2,NULL)")
            conn.execute("INSERT INTO back VALUES('u',1996,1,1,0,0,NULL,NULL)")
        self.service = OrdinaryTribulationService(self.game, self.player)

    def tearDown(self): self.tmp.cleanup()

    def test_success_is_atomic_and_idempotent(self):
        args = dict(expected_level="元婴境圆满", expected_exp=1000, expected_rate=40,
                    target_level="化神境初期", successful=True, new_rate=40,
                    occurred_at="2026-07-13 12:00:00.000000", power=999)
        self.assertEqual("applied", self.service.settle("op", "u", **args).status)
        self.assertEqual("duplicate", self.service.settle("op", "u", **args).status)
        replay = self.service.replay("op", "u")
        self.assertEqual("duplicate", replay.status)
        self.assertTrue(replay.successful)
        self.assertEqual("化神境初期", replay.target_level)
        self.assertEqual(
            "duplicate",
            self.service.settle(
                "op", "u", expected_level="changed", expected_exp=0,
                expected_rate=1, target_level="changed", successful=False,
                new_rate=1, occurred_at="changed",
            ).status,
        )
        self.assertEqual(
            "operation_conflict", self.service.replay("op", "other").status
        )
        with db_backend.connection(self.game) as conn:
            self.assertEqual(("化神境初期", 999), tuple(conn.execute("SELECT level,power FROM user_xiuxian").fetchone()))
            self.assertIsNone(conn.execute("SELECT * FROM user_tribulation").fetchone())

    def test_failure_consumes_protection_and_rolls_back_on_operation_error(self):
        with db_backend.transaction(self.game) as conn:
            conn.execute("CREATE TABLE ordinary_tribulation_operations(operation_id TEXT PRIMARY KEY,payload TEXT,successful INTEGER,rate INTEGER,item_used INTEGER)")
            conn.execute("CREATE TRIGGER fail_op BEFORE INSERT ON ordinary_tribulation_operations BEGIN SELECT RAISE(ABORT,'x'); END")
        with self.assertRaises(Exception):
            self.service.settle("bad", "u", expected_level="元婴境圆满", expected_exp=1000, expected_rate=40,
                target_level="化神境初期", successful=False, new_rate=50, occurred_at="now", consume_destiny_pill=True)
        with db_backend.connection(self.game) as conn:
            self.assertEqual(40, conn.execute("SELECT current_rate FROM user_tribulation").fetchone()[0])
            self.assertEqual(1, conn.execute("SELECT goods_num FROM back").fetchone()[0])


if __name__ == "__main__": unittest.main()
