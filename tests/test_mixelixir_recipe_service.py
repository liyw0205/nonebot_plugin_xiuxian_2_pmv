from __future__ import annotations

import tempfile
import unittest
from pathlib import Path

import nonebot

nonebot.init()

from nonebot_plugin_xiuxian_2.xiuxian.xiuxian_mixelixir.recipe_service import MixelixirRecipeService
from tests.test_db_backend import db_backend


class RecipeServiceTests(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.db = Path(self.tmp.name) / "game.db"
        with db_backend.transaction(self.db) as conn:
            conn.execute("CREATE TABLE user_xiuxian (user_id TEXT PRIMARY KEY, mixelixir_num INTEGER)")
            conn.execute("INSERT INTO user_xiuxian VALUES (%s,%s)", ("u", 1))
            conn.execute("CREATE TABLE back (user_id TEXT, goods_id INTEGER, goods_name TEXT, goods_type TEXT, goods_num INTEGER, UNIQUE(user_id,goods_id))")
            conn.execute("INSERT INTO back VALUES (%s,%s,%s,%s,%s)", ("u", 1, "草", "药材", 3))
            conn.execute("INSERT INTO back VALUES (%s,%s,%s,%s,%s)", ("u", 2, "炉", "炼丹炉", 1))
        self.service = MixelixirRecipeService(self.db)
        self.materials = [{"id": 1, "name": "草", "quantity": 3}]
        self.furnaces = [{"id": 2, "name": "炉", "quantity": 1}]
        self.recipes = [{"recipe_key": "r", "materials": [{"id": 1, "quantity": 2}], "furnace": {"id": 2}, "reward_id": 3, "reward_name": "丹"}]

    def tearDown(self):
        self.tmp.cleanup()

    def save(self, op="op", recipes=None):
        return self.service.save(op, "u", 1, self.materials, self.furnaces, recipes or self.recipes)

    def test_success_and_idempotency(self):
        self.assertEqual(self.save().status, "applied")
        self.assertEqual(self.save().status, "duplicate")
        # mutable recipe roll must not break same-op replay
        self.assertEqual(self.save(recipes=[{**self.recipes[0], "reward_id": 9}]).status, "duplicate")
        self.assertEqual(self.service.find("u", "r"), ("op", self.recipes[0]))

    def test_operation_failure_rolls_back_recipe(self):
        self.assertEqual(self.save("first").status, "applied")
        with db_backend.transaction(self.db) as conn:
            conn.execute("CREATE TRIGGER reject_recipe BEFORE INSERT ON mixelixir_recipe_save_operations BEGIN SELECT RAISE(ABORT,'reject'); END")
        with self.assertRaises(db_backend.IntegrityError):
            self.save("second", [{**self.recipes[0], "reward_id": 9}])
        self.assertEqual(self.service.find("u", "r")[0], "first")


if __name__ == "__main__":
    unittest.main()
