from __future__ import annotations

import tempfile
import unittest
from pathlib import Path

import nonebot

nonebot.init()

from nonebot_plugin_xiuxian_2.xiuxian.xiuxian_arena.challenge_cost_service import ArenaChallengeCostService
from tests.test_db_backend import db_backend


class ArenaChallengeCostServiceTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        root = Path(self.temp.name)
        self.game, self.player = root / "game.db", root / "player.db"
        with db_backend.transaction(self.game) as conn:
            conn.execute("CREATE TABLE user_xiuxian (user_id TEXT PRIMARY KEY,hp INTEGER,mp INTEGER,user_stamina INTEGER)")
            conn.execute("INSERT INTO user_xiuxian VALUES (%s,%s,%s,%s)", ("user", 50, 40, 8))
        with db_backend.transaction(self.player) as conn:
            conn.execute("CREATE TABLE arena (user_id TEXT PRIMARY KEY,daily_challenges_used INTEGER,daily_extra_challenges INTEGER,last_challenge_time TEXT)")
            conn.execute("INSERT INTO arena VALUES (%s,%s,%s,%s)", ("user", 2, 1, "old"))
        self.service = ArenaChallengeCostService(self.game, self.player)

    def tearDown(self):
        self.temp.cleanup()

    def consume(self, operation="cost", **overrides):
        values = dict(cap=11, stamina_cost=3, used=2, extra=1, hp=50, mp=40, stamina=8, last="old", now="new")
        values.update(overrides)
        return self.service.consume(operation, "user", "other", values["cap"], values["stamina_cost"], values["used"], values["extra"], values["hp"], values["mp"], values["stamina"], values["last"], values["now"])

    def state(self):
        with db_backend.connection(self.game) as conn:
            stamina = conn.execute("SELECT user_stamina FROM user_xiuxian WHERE user_id=%s", ("user",)).fetchone()[0]
        with db_backend.connection(self.player) as conn:
            arena = conn.execute("SELECT daily_challenges_used,last_challenge_time FROM arena WHERE user_id=%s", ("user",)).fetchone()
        return int(stamina), int(arena[0]), str(arena[1])

    def test_success_and_duplicate_use_one_snapshot(self):
        first, duplicate = self.consume("same"), self.consume("same")
        self.assertEqual((first.status, duplicate.status), ("applied", "duplicate"))
        self.assertEqual(self.state(), (5, 3, "new"))

    def test_rejection_and_stale_snapshot_change_nothing(self):
        self.assertEqual(self.consume("limit", cap=2).status, "limit_reached")
        self.assertEqual(self.consume("stale", hp=49).status, "state_changed")
        self.assertEqual(self.consume("stamina", stamina_cost=9).status, "stamina_insufficient")
        self.assertEqual(self.state(), (8, 2, "old"))

    def test_operation_failure_rolls_back_cost(self):
        with db_backend.transaction(self.game) as conn:
            conn.execute("CREATE TABLE arena_challenge_cost_operations (operation_id TEXT PRIMARY KEY,payload TEXT,used INTEGER,remaining INTEGER,stamina INTEGER,challenged_at TEXT,created_at TIMESTAMP)")
            conn.execute("CREATE TRIGGER fail_cost BEFORE INSERT ON arena_challenge_cost_operations BEGIN SELECT RAISE(ABORT,'failed'); END")
        with self.assertRaises(db_backend.IntegrityError):
            self.consume("rollback")
        self.assertEqual(self.state(), (8, 2, "old"))


if __name__ == "__main__":
    unittest.main()
