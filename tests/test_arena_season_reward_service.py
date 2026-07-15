from __future__ import annotations

import tempfile
import unittest
from pathlib import Path

import nonebot

nonebot.init()

from nonebot_plugin_xiuxian_2.xiuxian.xiuxian_arena.transaction_service import ArenaSeasonRewardService
from tests.test_db_backend import db_backend


class ArenaSeasonRewardServiceTests(unittest.TestCase):
    reset = {"daily_challenges_used": 3, "daily_extra_challenges": 1, "daily_challenge_buys": 1, "last_reset_date": "old", "last_buy_date": "old"}

    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        root = Path(self.temp.name)
        self.game, self.player = root / "game.db", root / "player.db"
        with db_backend.transaction(self.game) as conn:
            conn.execute("CREATE TABLE user_xiuxian (user_id TEXT PRIMARY KEY)")
            conn.execute("INSERT INTO user_xiuxian VALUES (%s)", ("user",))
            conn.execute("CREATE TABLE back (user_id TEXT,goods_id INTEGER,goods_name TEXT,goods_type TEXT,goods_num INTEGER,create_time TEXT,update_time TEXT,bind_num INTEGER,UNIQUE(user_id,goods_id))")
        with db_backend.transaction(self.player) as conn:
            conn.execute("CREATE TABLE arena (user_id TEXT PRIMARY KEY,score INTEGER,rank TEXT,honor_points INTEGER,total_honor_earned INTEGER,daily_challenges_used INTEGER,daily_extra_challenges INTEGER,daily_challenge_buys INTEGER,last_reset_date TEXT,last_buy_date TEXT)")
            conn.execute("INSERT INTO arena VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)", ("user", 2000, "黄金", 10, 20, 3, 1, 1, "old", "old"))
        self.service = ArenaSeasonRewardService(self.game, self.player)

    def tearDown(self):
        self.temp.cleanup()

    def claim(self, operation="reward", **overrides):
        values = dict(score=2000, rank="黄金", position=1, honor=10, total=20, reset=self.reset)
        values.update(overrides)
        return self.service.claim(operation, "user", "2026-07-13", values["score"], values["rank"], values["position"], values["honor"], values["total"], 300, 500, [{"id": 1, "name": "item", "type": "type", "amount": 2}], 99, expected_reset=values["reset"])

    def state(self):
        with db_backend.connection(self.player) as conn:
            arena = conn.execute("SELECT honor_points,total_honor_earned,daily_challenges_used,daily_extra_challenges,daily_challenge_buys,last_reset_date,last_buy_date FROM arena WHERE user_id=%s", ("user",)).fetchone()
        with db_backend.connection(self.game) as conn:
            item = conn.execute("SELECT goods_num,bind_num FROM back WHERE user_id=%s AND goods_id=%s", ("user", 1)).fetchone()
        return tuple(arena), tuple(item) if item else None

    def test_claim_grants_reward_marks_claimed_and_resets_together(self):
        result = self.claim()
        self.assertEqual((result.status, result.honor, result.honor_points, result.total_honor_earned), ("applied", 800, 810, 820))
        self.assertEqual(self.state(), ((810, 820, 0, 0, 0, "2026-07-13", "2026-07-13"), (2, 2)))

    def test_duplicate_conflict_and_other_operation_cannot_double_claim(self):
        first, duplicate = self.claim("same"), self.claim("same")
        conflict = self.claim("same", score=1999)
        another = self.claim("other", honor=810, total=820, reset={**self.reset, "daily_challenges_used": 0})
        self.assertEqual((first.status, duplicate.status, conflict.status, another.status), ("applied", "duplicate", "state_changed", "already_claimed"))
        self.assertEqual(self.state()[1], (2, 2))

    def test_stale_snapshot_and_operation_failure_roll_back(self):
        self.assertEqual(self.claim("stale", honor=9).status, "state_changed")
        with db_backend.transaction(self.game) as conn:
            conn.execute("CREATE TABLE arena_season_reward_operations (operation_id TEXT PRIMARY KEY,payload TEXT,season_key TEXT,user_id TEXT,honor INTEGER,honor_points INTEGER,total_honor_earned INTEGER,created_at TIMESTAMP,UNIQUE(season_key,user_id))")
            conn.execute("CREATE TRIGGER fail_reward BEFORE INSERT ON arena_season_reward_operations BEGIN SELECT RAISE(ABORT,'failed'); END")
        with self.assertRaises(db_backend.IntegrityError):
            self.claim("rollback")
        self.assertEqual(self.state(), ((10, 20, 3, 1, 1, "old", "old"), None))


if __name__ == "__main__":
    unittest.main()
