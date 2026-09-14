import sqlite3
import tempfile
import unittest
from datetime import datetime
from pathlib import Path

from ..repository import CombatSettlementSqlRepository


class Clock:
    def now(self):
        return datetime(2026, 9, 14, 10)


class CombatRepositoryTests(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        root = Path(self.tmp.name)
        self.game = root / "game.db"
        self.player = root / "player.db"
        with sqlite3.connect(self.game) as conn:
            conn.execute("CREATE TABLE user_xiuxian (user_id TEXT PRIMARY KEY, stone INTEGER)")
            conn.execute("INSERT INTO user_xiuxian VALUES ('u', 10)")
            conn.execute("CREATE TABLE back (user_id TEXT, goods_id INTEGER, goods_name TEXT, goods_type TEXT, goods_num INTEGER, create_time TEXT, update_time TEXT, bind_num INTEGER, UNIQUE(user_id, goods_id))")
            conn.execute("CREATE TABLE map_combat_settlement_operations (operation_id TEXT PRIMARY KEY, payload TEXT NOT NULL, stone INTEGER NOT NULL, rewards TEXT NOT NULL)")
        with sqlite3.connect(self.player) as conn:
            conn.execute("CREATE TABLE map_daily_limit (user_id TEXT PRIMARY KEY, date TEXT, combat_count INTEGER, resource_total_count INTEGER)")
            conn.execute("INSERT INTO map_daily_limit VALUES ('u', '2026-09-14', 0, 0)")
            conn.execute("CREATE TABLE map_combat_settlement (user_id TEXT PRIMARY KEY, snapshot TEXT)")
            conn.execute("INSERT INTO map_combat_settlement VALUES ('u', 'snap')")
        self.repo = CombatSettlementSqlRepository(self.game, self.player, clock=Clock())

    def tearDown(self):
        self.tmp.cleanup()

    def call(self, op="op", snapshot="snap", max_goods_num=10):
        return self.repo.settle(op, "u", {"date": "2026-09-14", "combat_count": "0", "resource_total_count": "0"}, snapshot, 4, 5, ({"id": 1, "name": "材料", "type": "材料", "amount": 2},), max_goods_num)

    def test_apply_and_replay(self):
        first = self.call()
        second = self.call()
        self.assertEqual((first["status"], first["stone"], first["rewards"]), ("applied", 5, ((1, 2),)))
        self.assertEqual(second["status"], "duplicate")

    def test_snapshot_change_does_not_write(self):
        result = self.call("changed", "other")
        self.assertEqual(result["status"], "state_changed")
        with sqlite3.connect(self.game) as conn:
            self.assertEqual(conn.execute("SELECT stone FROM user_xiuxian").fetchone()[0], 10)

    def test_inventory_full_does_not_write(self):
        result = self.call("full", max_goods_num=1)
        self.assertEqual(result["status"], "inventory_full")


if __name__ == "__main__":
    unittest.main()
