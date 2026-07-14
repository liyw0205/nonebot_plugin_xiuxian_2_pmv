from __future__ import annotations

import json
import tempfile
import unittest
from datetime import date
from pathlib import Path

import nonebot

nonebot.init()

from nonebot_plugin_xiuxian_2.xiuxian.xiuxian_arena.purchase_service import ArenaPurchaseService
from tests.test_db_backend import db_backend


class ArenaPurchaseServiceTests(unittest.TestCase):
    def setUp(self) -> None:
        self.temp_dir = tempfile.TemporaryDirectory()
        root = Path(self.temp_dir.name)
        self.game_database, self.player_database = root / "game.sqlite3", root / "player.sqlite3"
        with db_backend.transaction(self.game_database) as conn:
            conn.execute("CREATE TABLE user_xiuxian (user_id TEXT PRIMARY KEY)")
            conn.execute("INSERT INTO user_xiuxian VALUES (%s)", ("user",))
            conn.execute("CREATE TABLE back (user_id TEXT, goods_id INTEGER, goods_name TEXT, goods_type TEXT, goods_num INTEGER, create_time TEXT, update_time TEXT, bind_num INTEGER, UNIQUE(user_id, goods_id))")
        with db_backend.transaction(self.player_database) as conn:
            conn.execute("CREATE TABLE arena (user_id TEXT PRIMARY KEY, honor_points INTEGER, weekly_purchases TEXT)")
            conn.execute("INSERT INTO arena VALUES (%s, %s, %s)", ("user", 100, json.dumps({"_last_reset": "2026-07-13", "1": 1})))
        self.weekly = {"_last_reset": "2026-07-13", "1": 1}
        self.service = ArenaPurchaseService(self.game_database, self.player_database)

    def tearDown(self) -> None:
        self.temp_dir.cleanup()

    def purchase(self, operation_id="purchase", **overrides):
        values = dict(quantity=2, unit_cost=10, weekly_limit=5, expected_honor=100, weekly=self.weekly, max_goods=99)
        values.update(overrides)
        return self.service.purchase(operation_id, "user", 1, "item", "type", values["quantity"], values["unit_cost"], values["weekly_limit"], values["expected_honor"], values["weekly"], values["max_goods"], 1, values.get("today"))

    def state(self):
        with db_backend.connection(self.player_database) as conn:
            arena = conn.execute("SELECT honor_points, weekly_purchases FROM arena WHERE user_id=%s", ("user",)).fetchone()
        with db_backend.connection(self.game_database) as conn:
            item = conn.execute("SELECT goods_num, bind_num FROM back WHERE user_id=%s AND goods_id=%s", ("user", 1)).fetchone()
        return int(arena[0]), json.loads(str(arena[1])), tuple(map(int, item)) if item else None

    def test_success_deducts_honor_points_updates_limit_and_adds_item(self) -> None:
        result = self.purchase()
        self.assertEqual((result.status, result.cost, result.honor_points, result.purchased, result.inventory), ("applied", 20, 80, 3, 2))
        self.assertEqual(self.state(), (80, {"_last_reset": "2026-07-13", "1": 3}, (2, 2)))

    def test_rejections_and_stale_state_change_nothing(self) -> None:
        self.assertEqual(self.purchase("honor_points", unit_cost=60).status, "honor_insufficient")
        self.assertEqual(self.purchase("limit", quantity=5).status, "limit_reached")
        self.assertEqual(self.purchase("stale", expected_honor=99).status, "state_changed")
        with db_backend.transaction(self.game_database) as conn:
            conn.execute("INSERT INTO back VALUES (%s, %s, %s, %s, %s, %s, %s, %s)", ("user", 1, "item", "type", 99, "", "", 99))
        self.assertEqual(self.purchase("full").status, "inventory_full")
        self.assertEqual(self.state(), (100, self.weekly, (99, 99)))

    def test_duplicate_reuses_result_and_conflicting_retry_is_rejected(self) -> None:
        first, duplicate, conflict = self.purchase("repeat"), self.purchase("repeat"), self.purchase("repeat", quantity=1)
        self.assertEqual((first.status, duplicate.status, conflict.status), ("applied", "duplicate", "state_changed"))
        self.assertEqual(self.state(), (80, {"_last_reset": "2026-07-13", "1": 3}, (2, 2)))

    def test_operation_write_failure_rolls_back_everything(self) -> None:
        with db_backend.transaction(self.game_database) as conn:
            conn.execute("CREATE TABLE arena_purchase_operations (operation_id TEXT PRIMARY KEY, payload TEXT NOT NULL, quantity INTEGER NOT NULL, cost INTEGER NOT NULL, honor_points INTEGER NOT NULL, purchased INTEGER NOT NULL, inventory INTEGER NOT NULL, created_at TIMESTAMP)")
            conn.execute("CREATE TRIGGER fail_purchase BEFORE INSERT ON arena_purchase_operations BEGIN SELECT RAISE(ABORT, 'failed'); END")
        with self.assertRaises(db_backend.IntegrityError):
            self.purchase("rollback")
        self.assertEqual(self.state(), (100, self.weekly, None))

    def test_purchase_normalizes_new_week_inside_transaction(self) -> None:
        result = self.purchase("new-week", today=date(2026, 7, 20))
        self.assertEqual((result.status, result.purchased), ("applied", 2))
        self.assertEqual(
            self.state(),
            (80, {"_last_reset": "2026-07-20", "1": 2}, (2, 2)),
        )


if __name__ == "__main__":
    unittest.main()
