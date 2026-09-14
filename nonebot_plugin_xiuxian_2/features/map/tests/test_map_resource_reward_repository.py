import json
import sqlite3
import tempfile
import unittest
from pathlib import Path

from ..repository import MapResourceRewardSqlRepository


class MapResourceRewardRepositoryTests(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        root = Path(self.tmp.name)
        self.game = root / "game.sqlite3"
        self.player = root / "player.sqlite3"
        with sqlite3.connect(self.game) as conn:
            conn.execute("CREATE TABLE user_xiuxian(user_id TEXT PRIMARY KEY,user_stamina INTEGER,stone INTEGER)")
            conn.execute("INSERT INTO user_xiuxian VALUES('u',10,5)")
            conn.execute("CREATE TABLE back(user_id TEXT,goods_id INTEGER,goods_name TEXT,goods_type TEXT,goods_num INTEGER,create_time TEXT,update_time TEXT,bind_num INTEGER,UNIQUE(user_id,goods_id))")
            conn.execute("CREATE TABLE map_resource_reward_operations(operation_id TEXT PRIMARY KEY,payload TEXT NOT NULL,stone INTEGER NOT NULL,rewards TEXT NOT NULL)")
        with sqlite3.connect(self.player) as conn:
            conn.execute("CREATE TABLE map_interactive_actions(user_id TEXT PRIMARY KEY,action_id TEXT,status TEXT,state_json TEXT,settlement_json TEXT,ready_at TEXT,expires_at TEXT,cooldown_seconds INTEGER,updated_at TEXT)")
            settlement = {"daily": {"date": "2026-09-14", "gather_count": 2, "resource_total_count": 5}}
            conn.execute("INSERT INTO map_interactive_actions VALUES('u','a','active','{}',?,?,?,?,?)", (json.dumps(settlement, ensure_ascii=True, sort_keys=True, separators=(",", ":")), "2026-09-14 00:00:00", "2026-09-14 00:01:00", 20, "2026-09-14 00:00:00"))
            conn.execute("CREATE TABLE map_daily_limit(user_id TEXT PRIMARY KEY,date TEXT,gather_count INTEGER,resource_total_count INTEGER)")
            conn.execute("INSERT INTO map_daily_limit VALUES('u','2026-09-14',2,5)")
            conn.execute("CREATE TABLE map_cooldown(user_id TEXT PRIMARY KEY,gather_cd_until TEXT)")
        self.repo = MapResourceRewardSqlRepository(self.game, self.player)
        self.settlement = {"daily": {"date": "2026-09-14", "gather_count": 2, "resource_total_count": 5}}

    def tearDown(self):
        self.tmp.cleanup()

    def call(self, operation="r1", max_goods_num=99):
        return self.repo.settle(operation, "u", self.settlement["daily"], 10, 7, [{"id": 1, "name": "material", "type": "material", "amount": 2}], max_goods_num, action_id="a", action_settlement=self.settlement, cooldown_until="2026-09-14 00:02:00")

    def test_applies_assets_and_completes_action(self):
        self.assertEqual("applied", self.call()["status"])
        with sqlite3.connect(self.game) as conn:
            self.assertEqual((12, 2), conn.execute("SELECT stone,goods_num FROM user_xiuxian JOIN back USING(user_id)").fetchone())
        with sqlite3.connect(self.player) as conn:
            self.assertEqual(("completed", "2026-09-14 00:02:00"), conn.execute("SELECT status,gather_cd_until FROM map_interactive_actions JOIN map_cooldown USING(user_id)").fetchone())

    def test_replay_does_not_duplicate_assets(self):
        self.assertEqual("applied", self.call()["status"])
        self.assertEqual("duplicate", self.call()["status"])
        with sqlite3.connect(self.game) as conn:
            self.assertEqual(12, conn.execute("SELECT stone FROM user_xiuxian").fetchone()[0])

    def test_inventory_rejection_does_not_change_state(self):
        self.assertEqual("inventory_full", self.call(max_goods_num=1)["status"])
        with sqlite3.connect(self.game) as conn:
            self.assertEqual(5, conn.execute("SELECT stone FROM user_xiuxian").fetchone()[0])


if __name__ == "__main__":
    unittest.main()
