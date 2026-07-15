from __future__ import annotations

import json
import tempfile
import unittest
from pathlib import Path

import nonebot

nonebot.init()

from nonebot_plugin_xiuxian_2.xiuxian.xiuxian_mixelixir.transaction_service import MixelixirRefineCostService
from tests.test_db_backend import db_backend


class RefineCostServiceTests(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.db = Path(self.tmp.name) / "game.db"
        recipe = [{"recipe_key": "r", "materials": [{"id": 1, "quantity": 2}], "furnace": {"id": 2}, "reward_id": 3, "reward_name": "丹"}]
        with db_backend.transaction(self.db) as conn:
            conn.execute("CREATE TABLE user_xiuxian (user_id TEXT PRIMARY KEY, mixelixir_num INTEGER)")
            conn.execute("INSERT INTO user_xiuxian VALUES (%s,%s)", ("u", 1))
            conn.execute("CREATE TABLE back (user_id TEXT,goods_id INTEGER,goods_name TEXT,goods_type TEXT,goods_num INTEGER,UNIQUE(user_id,goods_id))")
            conn.execute("INSERT INTO back VALUES (%s,%s,%s,%s,%s)", ("u", 1, "草", "药材", 3))
            conn.execute("INSERT INTO back VALUES (%s,%s,%s,%s,%s)", ("u", 2, "炉", "炼丹炉", 1))
            conn.execute("CREATE TABLE mixelixir_recipe_sets (user_id TEXT PRIMARY KEY,recipe_set_id TEXT UNIQUE,daily_count INTEGER,materials_json TEXT,furnaces_json TEXT,recipes_json TEXT)")
            conn.execute("INSERT INTO mixelixir_recipe_sets VALUES (%s,%s,%s,%s,%s,%s)", ("u", "set", 1, json.dumps([[1, "草", 3]]), json.dumps([[2, "炉", 1]]), json.dumps(recipe, ensure_ascii=False)))
        self.service = MixelixirRefineCostService(self.db)

    def tearDown(self):
        self.tmp.cleanup()

    def start(self, op="op", quantity=2):
        return self.service.start(op, "u", "set", "r", 1, quantity, {"exp": 0}, {"exp": 1})

    def scalar(self, sql):
        with db_backend.connection(self.db) as conn:
            row = conn.execute(sql).fetchone()
            return row[0] if row else None

    def test_success_and_idempotency(self):
        self.assertEqual(self.start().status, "applied")
        self.assertEqual(self.start().status, "duplicate")
        self.assertEqual(self.start(quantity=3).status, "duplicate")
        self.assertEqual(self.scalar("SELECT goods_num FROM back WHERE goods_id=1"), 1)
        self.assertEqual(self.scalar("SELECT mixelixir_num FROM user_xiuxian"), 2)
        self.assertEqual(self.scalar("SELECT status FROM mixelixir_refine_tasks"), "ready")

    def test_task_failure_rolls_back_costs(self):
        self.start("bootstrap")
        with db_backend.transaction(self.db) as conn:
            conn.execute("DELETE FROM mixelixir_refine_tasks")
            conn.execute("DELETE FROM mixelixir_refine_cost_operations")
            conn.execute("UPDATE back SET goods_num=3 WHERE goods_id=1")
            conn.execute("UPDATE user_xiuxian SET mixelixir_num=1")
            conn.execute("CREATE TRIGGER reject_task BEFORE INSERT ON mixelixir_refine_tasks BEGIN SELECT RAISE(ABORT,'reject'); END")
        with self.assertRaises(db_backend.IntegrityError):
            self.start("rollback")
        self.assertEqual(self.scalar("SELECT goods_num FROM back WHERE goods_id=1"), 3)
        self.assertEqual(self.scalar("SELECT mixelixir_num FROM user_xiuxian"), 1)


if __name__ == "__main__":
    unittest.main()
