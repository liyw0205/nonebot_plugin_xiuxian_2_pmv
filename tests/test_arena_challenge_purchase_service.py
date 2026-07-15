from __future__ import annotations

import tempfile
import unittest
from datetime import date
from pathlib import Path

import nonebot

nonebot.init()

from nonebot_plugin_xiuxian_2.xiuxian.xiuxian_arena.transaction_service import (
    ArenaChallengePurchaseService,
)
from tests.test_db_backend import db_backend


class ArenaChallengePurchaseServiceTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        root = Path(self.temp.name)
        self.game, self.player = root / "game.db", root / "player.db"
        with db_backend.transaction(self.game) as conn:
            conn.execute("CREATE TABLE user_xiuxian(user_id TEXT PRIMARY KEY,stone INTEGER)")
            conn.execute("INSERT INTO user_xiuxian VALUES(%s,%s)", ("user", 100))
        with db_backend.transaction(self.player) as conn:
            conn.execute(
                "CREATE TABLE arena(user_id TEXT PRIMARY KEY,daily_challenge_buys INTEGER,"
                "daily_extra_challenges INTEGER,last_buy_date TEXT)"
            )
            conn.execute("INSERT INTO arena VALUES(%s,%s,%s,%s)", ("user", 1, 1, "2026-07-14"))
        self.service = ArenaChallengePurchaseService(self.game, self.player)

    def tearDown(self):
        self.temp.cleanup()

    def purchase(self, operation="purchase", **overrides):
        values = dict(
            amount=1,
            unit_cost=10,
            daily_limit=3,
            stone=100,
            bought=1,
            extra=1,
            last_buy="2026-07-14",
            today=date(2026, 7, 14),
        )
        values.update(overrides)
        return self.service.purchase(
            operation,
            "user",
            values["amount"],
            values["unit_cost"],
            values["daily_limit"],
            values["stone"],
            values["bought"],
            values["extra"],
            values["last_buy"],
            values["today"],
        )

    def state(self):
        with db_backend.connection(self.game) as conn:
            stone = conn.execute("SELECT stone FROM user_xiuxian WHERE user_id='user'").fetchone()[0]
        with db_backend.connection(self.player) as conn:
            arena = conn.execute(
                "SELECT daily_challenge_buys,daily_extra_challenges,last_buy_date "
                "FROM arena WHERE user_id='user'"
            ).fetchone()
        return int(stone), int(arena[0]), int(arena[1]), str(arena[2])

    def test_success_and_duplicate_replay_first_result(self):
        first = self.purchase("same")
        duplicate = self.purchase("same")
        conflict = self.purchase("same", amount=2)
        self.assertEqual((first.status, duplicate.status, conflict.status), ("applied", "duplicate", "state_changed"))
        self.assertEqual(self.state(), (90, 2, 2, "2026-07-14"))

    def test_new_day_uses_fresh_allowance_inside_transaction(self):
        result = self.purchase(
            "new-day",
            bought=1,
            extra=1,
            last_buy="2026-07-14",
            today=date(2026, 7, 15),
        )
        self.assertEqual((result.status, result.bought, result.extra), ("applied", 1, 1))
        self.assertEqual(self.state(), (90, 1, 1, "2026-07-15"))

    def test_rejections_and_stale_state_change_nothing(self):
        self.assertEqual(self.purchase("limit", bought=3, extra=3).status, "state_changed")
        self.assertEqual(self.purchase("stone", unit_cost=101).status, "stone_insufficient")
        self.assertEqual(self.purchase("stale", stone=99).status, "state_changed")
        self.assertEqual(self.state(), (100, 1, 1, "2026-07-14"))

    def test_operation_failure_rolls_back_all_state(self):
        with db_backend.transaction(self.game) as conn:
            conn.execute(
                "CREATE TABLE arena_challenge_purchase_operations("
                "operation_id TEXT PRIMARY KEY,payload TEXT NOT NULL,amount INTEGER NOT NULL,"
                "cost INTEGER NOT NULL,stone INTEGER NOT NULL,bought INTEGER NOT NULL,extra INTEGER NOT NULL)"
            )
            conn.execute(
                "CREATE TRIGGER fail_arena_buy BEFORE INSERT ON arena_challenge_purchase_operations "
                "BEGIN SELECT RAISE(ABORT,'failed'); END"
            )
        with self.assertRaises(db_backend.IntegrityError):
            self.purchase("rollback")
        self.assertEqual(self.state(), (100, 1, 1, "2026-07-14"))


if __name__ == "__main__":
    unittest.main()
