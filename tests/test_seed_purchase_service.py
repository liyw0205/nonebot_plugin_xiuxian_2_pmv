from __future__ import annotations

import tempfile
import unittest
from pathlib import Path

import nonebot

nonebot.init()

from nonebot_plugin_xiuxian_2.xiuxian.xiuxian_map.transaction_service import SeedPurchaseService
from tests.test_db_backend import db_backend


class SeedPurchaseServiceTests(unittest.TestCase):
    def setUp(self) -> None:
        self.temp_dir = tempfile.TemporaryDirectory()
        self.database = Path(self.temp_dir.name) / "game.sqlite3"
        with db_backend.transaction(self.database) as conn:
            conn.execute("CREATE TABLE user_xiuxian (user_id TEXT PRIMARY KEY, stone INTEGER)")
            conn.execute("INSERT INTO user_xiuxian VALUES (%s,%s)", ("user", 100))
            conn.execute("CREATE TABLE back (user_id TEXT,goods_id INTEGER,goods_name TEXT,goods_type TEXT,goods_num INTEGER,create_time TEXT,update_time TEXT,bind_num INTEGER,UNIQUE(user_id,goods_id))")
        self.service = SeedPurchaseService(self.database)

    def tearDown(self) -> None:
        self.temp_dir.cleanup()

    def purchase(self, operation_id="purchase", **overrides):
        values = dict(quantity=2, unit_cost=10, expected_stone=100, max_goods_num=99)
        values.update(overrides)
        return self.service.purchase(operation_id, "user", 1, "种子", values["quantity"], values["unit_cost"], values["expected_stone"], values["max_goods_num"])

    def state(self):
        with db_backend.connection(self.database) as conn:
            stone = conn.execute("SELECT stone FROM user_xiuxian WHERE user_id=%s", ("user",)).fetchone()
            item = conn.execute("SELECT goods_num,bind_num FROM back WHERE user_id=%s AND goods_id=%s", ("user", 1)).fetchone()
        return int(stone[0]), tuple(map(int, item)) if item else None

    def test_success_deducts_stone_and_adds_bound_seed(self) -> None:
        result = self.purchase()
        self.assertEqual((result.status, result.cost, result.stone, result.inventory), ("applied", 20, 80, 2))
        self.assertEqual(self.state(), (80, (2, 2)))

    def test_rejections_and_stale_state_change_nothing(self) -> None:
        self.assertEqual(self.purchase("stone", unit_cost=60).status, "stone_insufficient")
        self.assertEqual(self.purchase("stale", expected_stone=99).status, "state_changed")
        with db_backend.transaction(self.database) as conn:
            conn.execute("INSERT INTO back VALUES (%s,%s,%s,%s,%s,%s,%s,%s)", ("user", 1, "种子", "特殊物品", 99, "", "", 99))
        self.assertEqual(self.purchase("full").status, "inventory_full")
        self.assertEqual(self.state(), (100, (99, 99)))

    def test_duplicate_reuses_result_and_conflicting_retry_is_rejected(self) -> None:
        first, duplicate, conflict = self.purchase("repeat"), self.purchase("repeat"), self.purchase("repeat", quantity=1)
        self.assertEqual((first.status, duplicate.status, conflict.status), ("applied", "duplicate", "state_changed"))
        self.assertEqual(self.state(), (80, (2, 2)))

    def test_operation_write_failure_rolls_back_everything(self) -> None:
        with db_backend.transaction(self.database) as conn:
            conn.execute("CREATE TABLE map_seed_purchase_operations (operation_id TEXT PRIMARY KEY,payload TEXT NOT NULL,quantity INTEGER NOT NULL,cost INTEGER NOT NULL,stone INTEGER NOT NULL,inventory INTEGER NOT NULL,created_at TIMESTAMP)")
            conn.execute("CREATE TRIGGER fail_purchase BEFORE INSERT ON map_seed_purchase_operations BEGIN SELECT RAISE(ABORT, 'failed'); END")
        with self.assertRaises(db_backend.IntegrityError):
            self.purchase("rollback")
        self.assertEqual(self.state(), (100, None))


if __name__ == "__main__":
    unittest.main()
