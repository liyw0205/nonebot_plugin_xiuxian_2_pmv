from __future__ import annotations

import json
import tempfile
import unittest
from pathlib import Path

import nonebot

nonebot.init()

from nonebot_plugin_xiuxian_2.xiuxian.xiuxian_mixelixir.transaction_service import MixelixirRefineRewardService
from tests.test_db_backend import db_backend


class RefineRewardServiceTests(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        root = Path(self.tmp.name)
        self.game, self.player = root / "game.db", root / "player.db"
        expected = {"丹药控火": 1, "炼丹记录": {"丹": 1}, "炼丹经验": 2}
        updated = {"丹药控火": 2, "炼丹记录": {"丹": 2}, "炼丹经验": 3}
        with db_backend.transaction(self.game) as conn:
            conn.execute("CREATE TABLE user_xiuxian (user_id TEXT PRIMARY KEY)")
            conn.execute("INSERT INTO user_xiuxian VALUES (%s)", ("u",))
            conn.execute("CREATE TABLE back (user_id TEXT,goods_id INTEGER,goods_name TEXT,goods_type TEXT,goods_num INTEGER,bind_num INTEGER DEFAULT 0,UNIQUE(user_id,goods_id))")
            conn.execute("CREATE TABLE mixelixir_refine_tasks (task_id TEXT PRIMARY KEY,user_id TEXT,recipe_set_id TEXT,recipe_key TEXT,status TEXT,materials_json TEXT,reward_id INTEGER,reward_name TEXT,reward_quantity INTEGER,expected_mix_state TEXT,updated_mix_state TEXT,created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,claimed_at TIMESTAMP)")
            conn.execute("INSERT INTO mixelixir_refine_tasks (task_id,user_id,status,reward_id,reward_name,reward_quantity,expected_mix_state,updated_mix_state) VALUES (%s,%s,%s,%s,%s,%s,%s,%s)", ("task", "u", "ready", 3, "丹", 2, json.dumps(expected, ensure_ascii=False), json.dumps(updated, ensure_ascii=False)))
        with db_backend.transaction(self.player) as conn:
            conn.execute('CREATE TABLE mix_elixir_info (user_id TEXT PRIMARY KEY,"丹药控火" TEXT,"炼丹记录" TEXT,"炼丹经验" TEXT)')
            conn.execute('INSERT INTO mix_elixir_info VALUES (%s,%s,%s,%s)', ("u", "1", json.dumps({"丹": 1}, ensure_ascii=False, sort_keys=True), "2"))
        self.service = MixelixirRefineRewardService(self.game, self.player)

    def tearDown(self):
        self.tmp.cleanup()

    def claim(self, op="op", capacity=99):
        return self.service.claim(op, "u", "task", capacity)

    def scalar(self, db, sql):
        with db_backend.connection(db) as conn:
            row = conn.execute(sql).fetchone()
            return row[0] if row else None

    def test_success_and_idempotency(self):
        self.assertEqual(self.claim().status, "applied")
        self.assertEqual(self.claim().status, "duplicate")
        self.assertEqual(self.claim(capacity=100).status, "duplicate")
        self.assertEqual(self.scalar(self.game, "SELECT goods_num FROM back WHERE goods_id=3"), 2)
        self.assertEqual(self.scalar(self.game, "SELECT status FROM mixelixir_refine_tasks"), "claimed")
        self.assertEqual(self.scalar(self.player, 'SELECT "炼丹经验" FROM mix_elixir_info'), "3")

    def test_player_failure_rolls_back_reward_and_task(self):
        with db_backend.transaction(self.player) as conn:
            conn.execute("CREATE TRIGGER reject_state BEFORE UPDATE ON mix_elixir_info BEGIN SELECT RAISE(ABORT,'reject'); END")
        with self.assertRaises(db_backend.IntegrityError):
            self.claim("rollback")
        self.assertIsNone(self.scalar(self.game, "SELECT goods_num FROM back WHERE goods_id=3"))
        self.assertEqual(self.scalar(self.game, "SELECT status FROM mixelixir_refine_tasks"), "ready")
        self.assertEqual(self.scalar(self.player, 'SELECT "炼丹经验" FROM mix_elixir_info'), "2")


if __name__ == "__main__":
    unittest.main()
