import json
import tempfile
import unittest
from pathlib import Path

import nonebot

nonebot.init()

from nonebot_plugin_xiuxian_2.xiuxian.xiuxian_tower.settlement_service import TowerSettlementService
from tests.test_db_backend import db_backend


class TowerSettlementServiceTests(unittest.TestCase):
    def setUp(self) -> None:
        self.temp_dir = tempfile.TemporaryDirectory()
        root = Path(self.temp_dir.name)
        self.game_database, self.player_database = root / "game.sqlite3", root / "player.sqlite3"
        with db_backend.transaction(self.game_database) as conn:
            conn.execute("CREATE TABLE user_xiuxian (user_id TEXT PRIMARY KEY, stone INTEGER, exp INTEGER)")
            conn.execute("INSERT INTO user_xiuxian VALUES (%s, %s, %s)", ("user", 10, 100))
            conn.execute("CREATE TABLE back (user_id TEXT, goods_id INTEGER, goods_name TEXT, goods_type TEXT, goods_num INTEGER, create_time TEXT, update_time TEXT, bind_num INTEGER, UNIQUE(user_id, goods_id))")
        with db_backend.transaction(self.player_database) as conn:
            conn.execute("CREATE TABLE tower (user_id TEXT PRIMARY KEY, current_floor INTEGER, max_floor INTEGER, score INTEGER, weekly_purchases TEXT)")
            conn.execute("INSERT INTO tower VALUES (%s, %s, %s, %s, %s)", ("user", 9, 9, 50, "{}"))
        self.service = TowerSettlementService(self.game_database, self.player_database)
        self.expected = {"current_floor": 9, "max_floor": 9, "score": 50}

    def tearDown(self) -> None:
        self.temp_dir.cleanup()

    def settle(self, operation_id="settlement", **overrides):
        values = dict(expected=self.expected, floor=10, score=8, stone=20, exp=30, items=[{"id": 1, "name": "item", "type": "type", "amount": 1}], max_goods=99)
        values.update(overrides)
        return self.service.settle(operation_id, "user", values["expected"], values["floor"], values["score"], values["stone"], values["exp"], values["items"], values["max_goods"])

    def state(self):
        with db_backend.connection(self.player_database) as conn:
            tower = conn.execute("SELECT current_floor, max_floor, score FROM tower WHERE user_id=%s", ("user",)).fetchone()
        with db_backend.connection(self.game_database) as conn:
            user = conn.execute("SELECT stone, exp FROM user_xiuxian WHERE user_id=%s", ("user",)).fetchone()
            item = conn.execute("SELECT goods_num, bind_num FROM back WHERE user_id=%s AND goods_id=%s", ("user", 1)).fetchone()
        return tuple(map(int, tower)), tuple(map(int, user)), tuple(map(int, item)) if item else None

    def test_success_commits_tower_assets_and_item_together(self) -> None:
        result = self.settle()
        self.assertEqual((result.status, result.score, result.stone, result.exp), ("applied", 8, 20, 30))
        self.assertEqual(self.state(), ((10, 10, 58), (30, 130), (1, 1)))

    def test_duplicate_reuses_result_and_conflict_is_rejected(self) -> None:
        first, duplicate, conflict = self.settle("repeat"), self.settle("repeat"), self.settle("repeat", score=9)
        self.assertEqual((first.status, duplicate.status, conflict.status), ("applied", "duplicate", "state_changed"))
        self.assertEqual(self.state(), ((10, 10, 58), (30, 130), (1, 1)))

    def test_stale_state_and_full_inventory_leave_everything_unchanged(self) -> None:
        self.assertEqual(self.settle("stale", expected={"current_floor": 8, "max_floor": 9, "score": 50}).status, "state_changed")
        with db_backend.transaction(self.game_database) as conn:
            conn.execute("INSERT INTO back VALUES (%s, %s, %s, %s, %s, %s, %s, %s)", ("user", 1, "item", "type", 99, "", "", 99))
        self.assertEqual(self.settle("full").status, "inventory_full")
        self.assertEqual(self.state(), ((9, 9, 50), (10, 100), (99, 99)))

    def test_operation_write_failure_rolls_back_all_changes(self) -> None:
        with db_backend.transaction(self.game_database) as conn:
            conn.execute("CREATE TABLE tower_settlement_operations (operation_id TEXT PRIMARY KEY, payload TEXT NOT NULL, created_at TIMESTAMP)")
            conn.execute("CREATE TRIGGER fail_settlement BEFORE INSERT ON tower_settlement_operations BEGIN SELECT RAISE(ABORT, 'failed'); END")
        with self.assertRaises(db_backend.IntegrityError):
            self.settle("rollback")
        self.assertEqual(self.state(), ((9, 9, 50), (10, 100), None))


if __name__ == "__main__":
    unittest.main()
