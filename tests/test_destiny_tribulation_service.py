import tempfile
import unittest
from pathlib import Path

import nonebot

nonebot.init()

from nonebot_plugin_xiuxian_2.xiuxian.xiuxian_base.destiny_tribulation_service import DestinyTribulationService
from tests.test_db_backend import db_backend


class DestinyTribulationServiceTests(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory(); root = Path(self.tmp.name)
        self.game, self.player = root / "game.db", root / "player.db"
        with db_backend.transaction(self.game) as conn:
            conn.execute("CREATE TABLE user_xiuxian(user_id TEXT PRIMARY KEY,level TEXT,exp INTEGER,power INTEGER)")
            conn.execute("CREATE TABLE user_tribulation(user_id TEXT PRIMARY KEY,current_rate INTEGER)")
            conn.execute("CREATE TABLE back(user_id TEXT,goods_id INTEGER,goods_num INTEGER,bind_num INTEGER,day_num INTEGER,all_num INTEGER,update_time TEXT,action_time TEXT,PRIMARY KEY(user_id,goods_id))")
            conn.execute("INSERT INTO user_xiuxian VALUES('u','元婴境圆满',1000,10)")
            conn.execute("INSERT INTO user_tribulation VALUES('u',40)")
            conn.execute("INSERT INTO back VALUES('u',1997,1,1,0,0,NULL,NULL)")
        self.service = DestinyTribulationService(self.game, self.player)

    def tearDown(self): self.tmp.cleanup()

    def test_claim_promotes_consumes_and_records_once(self):
        args = dict(expected_level="元婴境圆满", expected_exp=1000, target_level="化神境初期", power=999, occurred_at="now")
        self.assertEqual("applied", self.service.settle("op", "u", **args).status)
        self.assertEqual("duplicate", self.service.settle("op", "u", **args).status)
        replay = self.service.replay("op", "u")
        self.assertEqual("duplicate", replay.status)
        self.assertEqual("化神境初期", replay.target_level)
        self.assertEqual(
            "duplicate",
            self.service.settle(
                "op", "u", expected_level="changed", expected_exp=0,
                target_level="changed", power=0, occurred_at="changed",
            ).status,
        )
        self.assertEqual(
            "operation_conflict", self.service.replay("op", "other").status
        )
        with db_backend.connection(self.game) as conn:
            self.assertEqual(("化神境初期", 999), tuple(conn.execute("SELECT level,power FROM user_xiuxian").fetchone()))
            self.assertEqual(0, conn.execute("SELECT goods_num FROM back").fetchone()[0])
            self.assertIsNone(conn.execute("SELECT * FROM user_tribulation").fetchone())

    def test_missing_item_changes_nothing(self):
        with db_backend.transaction(self.game) as conn: conn.execute("UPDATE back SET goods_num=0")
        result = self.service.settle("missing", "u", expected_level="元婴境圆满", expected_exp=1000, target_level="化神境初期", power=999, occurred_at="now")
        self.assertEqual("item_missing", result.status)
        with db_backend.connection(self.game) as conn: self.assertEqual("元婴境圆满", conn.execute("SELECT level FROM user_xiuxian").fetchone()[0])


if __name__ == "__main__": unittest.main()
