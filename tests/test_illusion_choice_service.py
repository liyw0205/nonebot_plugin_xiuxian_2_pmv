from __future__ import annotations

import tempfile
import unittest
from pathlib import Path

import nonebot

nonebot.init()

from nonebot_plugin_xiuxian_2.xiuxian.xiuxian_Illusion.choice_service import IllusionChoiceService
from tests.test_db_backend import db_backend


class IllusionChoiceServiceTests(unittest.TestCase):
    def setUp(self) -> None:
        self.temp_dir = tempfile.TemporaryDirectory()
        self.database = Path(self.temp_dir.name) / "game.sqlite3"
        with db_backend.transaction(self.database) as conn:
            conn.execute("CREATE TABLE user_xiuxian (user_id TEXT PRIMARY KEY, stone INTEGER, exp INTEGER)")
            conn.execute("INSERT INTO user_xiuxian VALUES (%s,%s,%s)", ("user", 100, 200))
            conn.execute("CREATE TABLE back (user_id TEXT, goods_id INTEGER, goods_name TEXT, goods_type TEXT, goods_num INTEGER, create_time TEXT, update_time TEXT, bind_num INTEGER, UNIQUE(user_id, goods_id))")
        self.service = IllusionChoiceService(self.database)
        self.item = {"id": 1, "name": "item", "type": "type", "amount": 2}

    def tearDown(self) -> None:
        self.temp_dir.cleanup()

    def choose(self, operation_id="choice", **overrides):
        values = dict(period="2026-07-13", choice=1, stone=10, exp=20, item=self.item, max_goods=99)
        values.update(overrides)
        return self.service.choose(operation_id, "user", values["period"], 0, values["choice"], "option", values["stone"], values["exp"], values["item"], values["max_goods"])

    def state(self):
        with db_backend.connection(self.database) as conn:
            user = conn.execute("SELECT stone, exp FROM user_xiuxian WHERE user_id=%s", ("user",)).fetchone()
            item = conn.execute("SELECT goods_num, bind_num FROM back WHERE user_id=%s AND goods_id=%s", ("user", 1)).fetchone()
            choices = int(conn.execute("SELECT COUNT(*) FROM illusion_choices").fetchone()[0]) if conn.table_exists("illusion_choices") else 0
            stats = int(conn.execute("SELECT COALESCE(SUM(choice_count),0) FROM illusion_choice_stats").fetchone()[0]) if conn.table_exists("illusion_choice_stats") else 0
        return tuple(map(int, user)), tuple(map(int, item)) if item else None, choices, stats

    def test_success_records_choice_stats_and_rewards(self) -> None:
        result = self.choose()
        self.assertEqual((result.status, result.choice_count, result.stone, result.exp, result.item_id), ("applied", 1, 10, 20, 1))
        self.assertEqual(self.state(), ((110, 220), (2, 2), 1, 1))

    def test_second_choice_in_period_is_rejected(self) -> None:
        self.choose("first")
        self.assertEqual(self.choose("second", choice=2, item=None, stone=0, exp=0).status, "already_chosen")
        self.assertEqual(self.state(), ((110, 220), (2, 2), 1, 1))

    def test_inventory_full_changes_nothing(self) -> None:
        with db_backend.transaction(self.database) as conn:
            conn.execute("INSERT INTO back VALUES (%s,%s,%s,%s,%s,%s,%s,%s)", ("user", 1, "item", "type", 99, "", "", 99))
        self.assertEqual(self.choose("full").status, "inventory_full")
        self.assertEqual(self.state(), ((100, 200), (99, 99), 0, 0))

    def test_duplicate_reuses_result_and_conflict_is_rejected(self) -> None:
        first = self.choose("repeat")
        # mutable reward amounts must not break same-op replay (identity is user/period/question/choice)
        duplicate = self.choose("repeat", stone=11, exp=0, item=None)
        self.assertEqual((first.status, duplicate.status), ("applied", "duplicate"))
        self.assertEqual((duplicate.stone, duplicate.exp, duplicate.item_id), (10, 20, 1))
        self.assertEqual(self.state(), ((110, 220), (2, 2), 1, 1))
        prior = self.service.get_result("repeat")
        self.assertIsNotNone(prior)
        self.assertEqual(prior.status, "duplicate")
        self.assertEqual(prior.stone, 10)
        # different choice index is identity conflict
        conflict = self.choose("repeat", choice=2, stone=10, exp=20)
        self.assertEqual(conflict.status, "state_changed")

    def test_operation_failure_rolls_back_everything(self) -> None:
        with db_backend.transaction(self.database) as conn:
            conn.execute("CREATE TABLE illusion_choice_operations (operation_id TEXT PRIMARY KEY, payload TEXT NOT NULL, choice_count INTEGER NOT NULL, created_at TIMESTAMP)")
            conn.execute("CREATE TRIGGER fail_choice BEFORE INSERT ON illusion_choice_operations BEGIN SELECT RAISE(ABORT, 'failed'); END")
        with self.assertRaises(db_backend.IntegrityError):
            self.choose("rollback")
        self.assertEqual(self.state(), ((100, 200), None, 0, 0))


if __name__ == "__main__":
    unittest.main()
