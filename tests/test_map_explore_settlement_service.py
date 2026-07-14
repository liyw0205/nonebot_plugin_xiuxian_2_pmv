from __future__ import annotations

import tempfile
import unittest
from pathlib import Path

import nonebot

nonebot.init()

from nonebot_plugin_xiuxian_2.xiuxian.xiuxian_map.explore_settlement_service import MapExploreSettlementService
from tests.test_db_backend import db_backend


class MapExploreSettlementServiceTests(unittest.TestCase):
    def setUp(self):
        self.temp_dir = tempfile.TemporaryDirectory()
        root = Path(self.temp_dir.name)
        self.game, self.player = root / "game.sqlite3", root / "player.sqlite3"
        with db_backend.transaction(self.game) as conn:
            conn.execute("CREATE TABLE user_xiuxian (user_id TEXT PRIMARY KEY, stone INTEGER)")
            conn.execute("INSERT INTO user_xiuxian VALUES (%s,%s)", ("u", 10))
            conn.execute("CREATE TABLE back (user_id TEXT,goods_id INTEGER,goods_name TEXT,goods_type TEXT,goods_num INTEGER,create_time TEXT,update_time TEXT,bind_num INTEGER,UNIQUE(user_id,goods_id))")
        with db_backend.transaction(self.player) as conn:
            conn.execute("CREATE TABLE map_daily_limit (user_id TEXT PRIMARY KEY,date TEXT,explore_count INTEGER,resource_total_count INTEGER)")
            conn.execute("INSERT INTO map_daily_limit VALUES (%s,%s,%s,%s)", ("u", "2026-07-13", 2, 5))
            conn.execute("CREATE TABLE map_explore_status (user_id TEXT PRIMARY KEY,running INTEGER,node_type TEXT,node_name TEXT,start_time TEXT,duration_min INTEGER,max_duration_min INTEGER,interval_min INTEGER,settlement TEXT)")
            conn.execute("INSERT INTO map_explore_status VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s)", ("u", 1, "矿脉", "矿洞", "2026-07-13 10:00:00", 60, 120, 20, "snapshot"))
        self.service = MapExploreSettlementService(self.game, self.player)
        self.state = {
            "running": 1, "node_type": "矿脉", "node_name": "矿洞", "start_time": "2026-07-13 10:00:00",
            "duration_min": 60, "max_duration_min": 120, "interval_min": 20, "settlement": "snapshot",
        }
        self.daily = {"date": "2026-07-13", "explore_count": 2, "resource_total_count": 5}
        self.items = [{"id": 1, "name": "材料", "type": "材料", "amount": 2}]

    def tearDown(self):
        self.temp_dir.cleanup()

    def settle(self, operation_id="op", **overrides):
        values = dict(limit=5, stone=7, items=self.items, max_goods=99)
        values.update(overrides)
        return self.service.settle(operation_id, "u", self.state, self.daily, values["limit"], values["stone"], values["items"], values["max_goods"])

    def current_state(self):
        with db_backend.connection(self.game) as conn:
            stone = conn.execute("SELECT stone FROM user_xiuxian WHERE user_id=%s", ("u",)).fetchone()
            item = conn.execute("SELECT goods_num,bind_num FROM back WHERE user_id=%s AND goods_id=%s", ("u", 1)).fetchone()
        with db_backend.connection(self.player) as conn:
            daily = conn.execute("SELECT explore_count,resource_total_count FROM map_daily_limit WHERE user_id=%s", ("u",)).fetchone()
            running = conn.execute("SELECT running FROM map_explore_status WHERE user_id=%s", ("u",)).fetchone()
        return int(stone[0]), tuple(map(int, daily)), int(running[0]), tuple(map(int, item)) if item else None

    def test_success_clears_status_and_grants_rewards_together(self):
        result = self.settle()
        self.assertEqual((result.status, result.stone, result.rewards), ("applied", 7, ((1, 2),)))
        self.assertEqual(self.current_state(), (17, (3, 6), 0, (2, 2)))

    def test_duplicate_reuses_result_and_stale_state_changes_nothing(self):
        first, duplicate = self.settle("repeat"), self.settle("repeat")
        self.assertEqual((first.status, duplicate.status), ("applied", "duplicate"))
        self.assertEqual(self.current_state(), (17, (3, 6), 0, (2, 2)))
        self.setUp()
        self.state["settlement"] = "other"
        self.assertEqual(self.settle("stale").status, "state_changed")
        self.assertEqual(self.current_state(), (10, (2, 5), 1, None))

    def test_limit_and_inventory_rejections_preserve_status(self):
        self.assertEqual(self.settle("limit", limit=2).status, "limit_reached")
        with db_backend.transaction(self.game) as conn:
            conn.execute("INSERT INTO back VALUES (%s,%s,%s,%s,%s,%s,%s,%s)", ("u", 1, "材料", "材料", 99, "", "", 99))
        self.assertEqual(self.settle("full").status, "inventory_full")
        self.assertEqual(self.current_state(), (10, (2, 5), 1, (99, 99)))

    def test_operation_write_failure_rolls_back_everything(self):
        with db_backend.transaction(self.game) as conn:
            conn.execute("CREATE TABLE map_explore_settlement_operations (operation_id TEXT PRIMARY KEY,payload TEXT NOT NULL,stone INTEGER NOT NULL,rewards TEXT NOT NULL,created_at TIMESTAMP)")
            conn.execute("CREATE TRIGGER fail_settlement BEFORE INSERT ON map_explore_settlement_operations BEGIN SELECT RAISE(ABORT, 'failed'); END")
        with self.assertRaises(db_backend.IntegrityError):
            self.settle("rollback")
        self.assertEqual(self.current_state(), (10, (2, 5), 1, None))

    def test_legacy_reward_plan_schema_is_migrated_and_settled(self):
        with db_backend.transaction(self.player) as conn:
            conn.execute("DROP TABLE map_explore_status")
            conn.execute(
                "CREATE TABLE map_explore_status ("
                "user_id TEXT PRIMARY KEY,running INTEGER,node_type TEXT,node_name TEXT,start_time TEXT,"
                "duration_min INTEGER,max_duration_min INTEGER,interval_min INTEGER,reward_plan TEXT)"
            )
            conn.execute(
                "INSERT INTO map_explore_status VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s)",
                ("u", 1, "矿脉", "矿洞", "2026-07-13 10:00:00", 60, 120, 20, ' {"value":1} '),
            )
        self.state["settlement"] = '{"value": 1}'
        result = self.settle("legacy-settle")
        self.assertEqual((result.status, result.stone, result.rewards), ("applied", 7, ((1, 2),)))
        self.assertEqual(self.current_state(), (17, (3, 6), 0, (2, 2)))
        with db_backend.connection(self.player) as conn:
            row = conn.execute(
                "SELECT settlement,reward_plan FROM map_explore_status WHERE user_id=%s", ("u",)
            ).fetchone()
        self.assertEqual(tuple(row), ("", ""))


if __name__ == "__main__":
    unittest.main()
