import tempfile
import unittest
from pathlib import Path

import nonebot

nonebot.init()

from nonebot_plugin_xiuxian_2.xiuxian.xiuxian_dungeon.purchase_service import DungeonPurchaseService
from tests.test_db_backend import db_backend


class DungeonPurchaseServiceTests(unittest.TestCase):
    def setUp(self):
        self.temp_dir = tempfile.TemporaryDirectory()
        self.database = Path(self.temp_dir.name) / "game.sqlite3"
        with db_backend.transaction(self.database) as conn:
            conn.execute("CREATE TABLE user_xiuxian (user_id TEXT PRIMARY KEY,stone INTEGER)")
            conn.execute("INSERT INTO user_xiuxian VALUES (%s,%s)", ("u", 100))
            conn.execute("CREATE TABLE back (user_id TEXT,goods_id INTEGER,goods_name TEXT,goods_type TEXT,goods_num INTEGER,create_time TEXT,update_time TEXT,bind_num INTEGER,UNIQUE(user_id,goods_id))")
        self.service = DungeonPurchaseService(self.database)

    def tearDown(self):
        self.temp_dir.cleanup()

    def purchase(self, operation="buy", **overrides):
        values = {"quantity": 2, "unit_cost": 10, "expected_stone": 100, "max_goods": 99}
        values.update(overrides)
        return self.service.purchase(operation, "u", 1, "item", "type", values["quantity"], values["unit_cost"], values["expected_stone"], values["max_goods"], 1)

    def state(self):
        with db_backend.connection(self.database) as conn:
            stone = int(conn.execute("SELECT stone FROM user_xiuxian WHERE user_id=%s", ("u",)).fetchone()[0])
            item = conn.execute("SELECT goods_num,bind_num FROM back WHERE user_id=%s AND goods_id=%s", ("u", 1)).fetchone()
        return stone, tuple(map(int, item)) if item else None

    def test_purchase_deducts_stone_and_adds_item(self):
        result = self.purchase()
        self.assertEqual((result.status, result.cost, result.stone, result.inventory), ("applied", 20, 80, 2))
        self.assertEqual(self.state(), (80, (2, 2)))

    def test_rejections_and_duplicate_change_nothing_extra(self):
        self.assertEqual(self.purchase("poor", unit_cost=60).status, "stone_insufficient")
        self.assertEqual(self.purchase("stale", expected_stone=99).status, "state_changed")
        self.assertEqual(self.purchase("repeat").status, "applied")
        self.assertEqual(self.purchase("repeat").status, "duplicate")
        self.assertEqual(self.state(), (80, (2, 2)))

    def test_operation_failure_rolls_back_resources(self):
        with db_backend.transaction(self.database) as conn:
            conn.execute("CREATE TABLE dungeon_purchase_operations (operation_id TEXT PRIMARY KEY,payload TEXT,quantity INTEGER,cost INTEGER,stone INTEGER,inventory INTEGER,created_at TIMESTAMP)")
            conn.execute("CREATE TRIGGER fail_purchase BEFORE INSERT ON dungeon_purchase_operations BEGIN SELECT RAISE(ABORT, 'failed'); END")
        with self.assertRaises(db_backend.IntegrityError):
            self.purchase("rollback")
        self.assertEqual(self.state(), (100, None))


if __name__ == "__main__":
    unittest.main()
