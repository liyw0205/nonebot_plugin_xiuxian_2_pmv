from __future__ import annotations

import tempfile
import unittest
from pathlib import Path

import nonebot

nonebot.init()

from nonebot_plugin_xiuxian_2.xiuxian.xiuxian_map.combat_settlement_service import MapCombatSettlementService
from tests.test_db_backend import db_backend


class MapCombatSettlementServiceTests(unittest.TestCase):
    def setUp(self):
        self.temp_dir = tempfile.TemporaryDirectory()
        root = Path(self.temp_dir.name)
        self.game, self.player = root / "game.sqlite3", root / "player.sqlite3"
        with db_backend.transaction(self.game) as conn:
            conn.execute("CREATE TABLE user_xiuxian (user_id TEXT PRIMARY KEY, stone INTEGER)")
            conn.execute("INSERT INTO user_xiuxian VALUES (%s,%s)", ("u", 10))
            conn.execute("CREATE TABLE back (user_id TEXT,goods_id INTEGER,goods_name TEXT,goods_type TEXT,goods_num INTEGER,create_time TEXT,update_time TEXT,bind_num INTEGER,UNIQUE(user_id,goods_id))")
        with db_backend.transaction(self.player) as conn:
            conn.execute("CREATE TABLE map_daily_limit (user_id TEXT PRIMARY KEY,date TEXT,combat_count INTEGER,resource_total_count INTEGER)")
            conn.execute("INSERT INTO map_daily_limit VALUES (%s,%s,%s,%s)", ("u", "2026-07-13", 2, 5))
            conn.execute("CREATE TABLE map_combat_settlement (user_id TEXT PRIMARY KEY,snapshot TEXT)")
            conn.execute("INSERT INTO map_combat_settlement VALUES (%s,%s)", ("u", "snapshot"))
        self.service = MapCombatSettlementService(self.game, self.player)
        self.daily = {"date": "2026-07-13", "combat_count": 2, "resource_total_count": 5}
        self.items = [{"id": 1, "name": "材料", "type": "材料", "amount": 2}]

    def tearDown(self):
        self.temp_dir.cleanup()

    def settle(self, operation_id="op", **overrides):
        values = dict(snapshot="snapshot", limit=7, stone=7, items=self.items, max_goods=99)
        values.update(overrides)
        return self.service.settle(operation_id, "u", self.daily, values["snapshot"], values["limit"], values["stone"], values["items"], values["max_goods"])

    def state(self):
        with db_backend.connection(self.game) as conn:
            stone = conn.execute("SELECT stone FROM user_xiuxian WHERE user_id=%s", ("u",)).fetchone()
            item = conn.execute("SELECT goods_num,bind_num FROM back WHERE user_id=%s AND goods_id=%s", ("u", 1)).fetchone()
        with db_backend.connection(self.player) as conn:
            daily = conn.execute("SELECT combat_count,resource_total_count FROM map_daily_limit WHERE user_id=%s", ("u",)).fetchone()
            snapshot = conn.execute("SELECT snapshot FROM map_combat_settlement WHERE user_id=%s", ("u",)).fetchone()
        return int(stone[0]), tuple(map(int, daily)), str(snapshot[0]), tuple(map(int, item)) if item else None

    def test_success_updates_counters_rewards_and_clears_snapshot(self):
        result = self.settle()
        self.assertEqual((result.status, result.stone, result.rewards), ("applied", 7, ((1, 2),)))
        self.assertEqual(self.state(), (17, (3, 6), "", (2, 2)))

    def test_duplicate_and_rejections_preserve_state(self):
        first, duplicate = self.settle("repeat"), self.settle("repeat")
        self.assertEqual((first.status, duplicate.status), ("applied", "duplicate"))
        self.setUp()
        self.assertEqual(self.settle("limit", limit=2).status, "limit_reached")
        self.assertEqual(self.settle("stale", snapshot="other").status, "state_changed")
        self.assertEqual(self.state(), (10, (2, 5), "snapshot", None))

    def test_inventory_and_operation_failure_roll_back_everything(self):
        with db_backend.transaction(self.game) as conn:
            conn.execute("INSERT INTO back VALUES (%s,%s,%s,%s,%s,%s,%s,%s)", ("u", 1, "材料", "材料", 99, "", "", 99))
        self.assertEqual(self.settle("full").status, "inventory_full")
        self.assertEqual(self.state(), (10, (2, 5), "snapshot", (99, 99)))
        self.setUp()
        with db_backend.transaction(self.game) as conn:
            conn.execute("CREATE TABLE map_combat_settlement_operations (operation_id TEXT PRIMARY KEY,payload TEXT NOT NULL,stone INTEGER NOT NULL,rewards TEXT NOT NULL,created_at TIMESTAMP)")
            conn.execute("CREATE TRIGGER fail_combat BEFORE INSERT ON map_combat_settlement_operations BEGIN SELECT RAISE(ABORT, 'failed'); END")
        with self.assertRaises(db_backend.IntegrityError):
            self.settle("rollback")
        self.assertEqual(self.state(), (10, (2, 5), "snapshot", None))


if __name__ == "__main__":
    unittest.main()
