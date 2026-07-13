from __future__ import annotations

import json
import tempfile
import unittest
from pathlib import Path

import nonebot

nonebot.init()

from nonebot_plugin_xiuxian_2.xiuxian.xiuxian_tower.purchase_service import TowerPurchaseService, normalize_weekly_purchases
from tests.test_db_backend import db_backend


class TowerPurchaseServiceTests(unittest.TestCase):
    def setUp(self) -> None:
        self.temp_dir = tempfile.TemporaryDirectory()
        root = Path(self.temp_dir.name)
        self.game_database = root / "game.sqlite3"
        self.player_database = root / "player.sqlite3"
        with db_backend.transaction(self.game_database) as conn:
            conn.execute("CREATE TABLE user_xiuxian (user_id TEXT PRIMARY KEY)")
            conn.execute("INSERT INTO user_xiuxian VALUES (%s)", ("user",))
            conn.execute(
                "CREATE TABLE back (user_id TEXT, goods_id INTEGER, goods_name TEXT, goods_type TEXT, "
                "goods_num INTEGER, create_time TEXT, update_time TEXT, bind_num INTEGER, "
                "UNIQUE(user_id, goods_id))"
            )
        with db_backend.transaction(self.player_database) as conn:
            conn.execute("CREATE TABLE tower (user_id TEXT PRIMARY KEY, score INTEGER, weekly_purchases TEXT)")
            conn.execute("INSERT INTO tower VALUES (%s, %s, %s)", ("user", 100, json.dumps({"_last_reset": "2026-07-13", "1": 1})))
        self.weekly = {"_last_reset": "2026-07-13", "1": 1}
        self.service = TowerPurchaseService(self.game_database, self.player_database)

    def tearDown(self) -> None:
        self.temp_dir.cleanup()

    def purchase(self, operation_id="purchase", **overrides):
        values = dict(quantity=2, unit_cost=10, weekly_limit=5, expected_score=100, weekly=self.weekly, max_goods=99)
        values.update(overrides)
        return self.service.purchase(operation_id, "user", 1, "item", "type", values["quantity"], values["unit_cost"], values["weekly_limit"], values["expected_score"], values["weekly"], values["max_goods"], 1)

    def state(self):
        with db_backend.connection(self.player_database) as conn:
            tower = conn.execute("SELECT score, weekly_purchases FROM tower WHERE user_id=%s", ("user",)).fetchone()
        with db_backend.connection(self.game_database) as conn:
            item = conn.execute("SELECT goods_num, bind_num FROM back WHERE user_id=%s AND goods_id=%s", ("user", 1)).fetchone()
        return int(tower[0]), json.loads(str(tower[1])), tuple(map(int, item)) if item else None

    def test_success_deducts_score_updates_limit_and_adds_item(self) -> None:
        result = self.purchase()
        self.assertEqual((result.status, result.cost, result.score, result.purchased, result.inventory), ("applied", 20, 80, 3, 2))
        self.assertEqual(self.state(), (80, {"_last_reset": "2026-07-13", "1": 3}, (2, 2)))

    def test_business_rejections_change_nothing(self) -> None:
        self.assertEqual(self.purchase("score", expected_score=100, unit_cost=60).status, "score_insufficient")
        self.assertEqual(self.purchase("limit", quantity=5).status, "limit_reached")
        with db_backend.transaction(self.game_database) as conn:
            conn.execute("INSERT INTO back VALUES (%s, %s, %s, %s, %s, %s, %s, %s)", ("user", 1, "item", "type", 99, "", "", 99))
        self.assertEqual(self.purchase("full").status, "inventory_full")
        self.assertEqual(self.state(), (100, self.weekly, (99, 99)))

    def test_stale_state_changes_nothing(self) -> None:
        result = self.purchase(expected_score=99)
        self.assertEqual(result.status, "state_changed")
        self.assertEqual(self.state(), (100, self.weekly, None))

    def test_duplicate_reuses_result_and_conflict_is_rejected(self) -> None:
        first = self.purchase("repeat")
        duplicate = self.purchase("repeat")
        conflict = self.purchase("repeat", quantity=1)
        self.assertEqual((first.status, duplicate.status, conflict.status), ("applied", "duplicate", "state_changed"))
        self.assertEqual((duplicate.score, duplicate.inventory), (80, 2))
        self.assertEqual(self.state(), (80, {"_last_reset": "2026-07-13", "1": 3}, (2, 2)))

    def test_stale_week_snapshot_is_reset_inside_purchase(self) -> None:
        stale = {"_last_reset": "2020-01-01", "1": 5}
        with db_backend.transaction(self.player_database) as conn:
            conn.execute("UPDATE tower SET weekly_purchases=%s WHERE user_id=%s", (json.dumps(stale), "user"))
        result = self.purchase("new-week", weekly=stale)
        self.assertEqual((result.status, result.purchased), ("applied", 2))
        score, weekly, inventory = self.state()
        self.assertEqual((score, weekly["1"], inventory), (80, 2, (2, 2)))
        self.assertNotEqual(weekly["_last_reset"], "2020-01-01")

    def test_week_normalization_is_stable_within_current_week(self) -> None:
        self.assertEqual(normalize_weekly_purchases(self.weekly), self.weekly)

    def test_operation_failure_rolls_back_score_limit_and_inventory(self) -> None:
        with db_backend.transaction(self.game_database) as conn:
            conn.execute(
                "CREATE TABLE tower_purchase_operations (operation_id TEXT PRIMARY KEY, payload TEXT NOT NULL, "
                "quantity INTEGER NOT NULL, cost INTEGER NOT NULL, score INTEGER NOT NULL, purchased INTEGER NOT NULL, "
                "inventory INTEGER NOT NULL, created_at TIMESTAMP)"
            )
            conn.execute("CREATE TRIGGER fail_purchase BEFORE INSERT ON tower_purchase_operations BEGIN SELECT RAISE(ABORT, 'failed'); END")
        with self.assertRaises(db_backend.IntegrityError):
            self.purchase("rollback")
        self.assertEqual(self.state(), (100, self.weekly, None))


if __name__ == "__main__":
    unittest.main()
