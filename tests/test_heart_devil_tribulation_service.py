import tempfile
import unittest
from pathlib import Path

import nonebot

nonebot.init()

from nonebot_plugin_xiuxian_2.xiuxian.xiuxian_base.heart_devil_tribulation_service import HeartDevilTribulationService
from tests.test_db_backend import db_backend


class HeartDevilTribulationServiceTests(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory(); root = Path(self.tmp.name)
        self.game, self.player = root / "game.db", root / "player.db"
        with db_backend.transaction(self.game) as conn:
            conn.execute("CREATE TABLE user_tribulation(user_id TEXT PRIMARY KEY,current_rate INTEGER,heart_devil_count INTEGER,last_time TEXT)")
            conn.execute("CREATE TABLE back(user_id TEXT,goods_id INTEGER,goods_num INTEGER,bind_num INTEGER,day_num INTEGER,all_num INTEGER,update_time TEXT,action_time TEXT,PRIMARY KEY(user_id,goods_id))")
            conn.execute("INSERT INTO user_tribulation VALUES('u',40,2,NULL)")
            conn.execute("INSERT INTO back VALUES('u',1996,1,1,0,0,NULL,NULL)")
        self.service = HeartDevilTribulationService(self.game, self.player)

    def tearDown(self): self.tmp.cleanup()

    def test_resolved_victory_updates_rate_count_and_stats_once(self):
        args = dict(expected_rate=40, expected_count=2, successful=True, new_rate=60, occurred_at="now", devil_name="贪欲心魔")
        self.assertEqual("applied", self.service.settle("op", "u", **args).status)
        self.assertEqual("duplicate", self.service.settle("op", "u", **args).status)
        with db_backend.connection(self.game) as conn: self.assertEqual((60, 3), tuple(conn.execute("SELECT current_rate,heart_devil_count FROM user_tribulation").fetchone()))
        with db_backend.connection(self.player) as conn: self.assertEqual((1, 1), tuple(conn.execute('SELECT "心魔劫次数","心魔劫成功" FROM statistics').fetchone()))

    def test_protected_failure_consumes_item_atomically(self):
        result = self.service.settle("loss", "u", expected_rate=40, expected_count=2, successful=False,
            new_rate=40, occurred_at="now", consume_destiny_pill=True)
        self.assertEqual("applied", result.status)
        with db_backend.connection(self.game) as conn:
            self.assertEqual(0, conn.execute("SELECT goods_num FROM back").fetchone()[0])
            self.assertEqual((40, 3), tuple(conn.execute("SELECT current_rate,heart_devil_count FROM user_tribulation").fetchone()))


if __name__ == "__main__": unittest.main()
