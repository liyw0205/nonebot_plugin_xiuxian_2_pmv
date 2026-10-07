from __future__ import annotations

import tempfile
import unittest
from pathlib import Path

import nonebot

nonebot.init()

from nonebot_plugin_xiuxian_2.features.dufang.application import DufangApplication
from nonebot_plugin_xiuxian_2.features.dufang.migrations import (
    apply_dufang_bet_payout,
    apply_dufang_player_receipts,
    apply_dufang_resolution,
    apply_dufang_share_player,
)
from nonebot_plugin_xiuxian_2.infrastructure.database import DatabaseUnitOfWork
from nonebot_plugin_xiuxian_2.plugin import apply_platform_schema
from tests.test_db_backend import db_backend


WIN_PLAN = {
    "entity": {"name": "sealed", "desc": "sealed"},
    "process": "open",
    "result_type": "success",
    "event": {"title": "found", "desc": "found", "outcome": "win"},
    "payout_outcome": "win",
    "gain": 600,
    "requested_loss": 0,
    "sharing": None,
}


class DufangApplicationTests(unittest.TestCase):
    def setUp(self) -> None:
        self.temp = tempfile.TemporaryDirectory()
        root = Path(self.temp.name)
        self.game = root / "game.db"
        self.player = root / "player.db"
        with DatabaseUnitOfWork(self.game) as uow:
            uow.execute("CREATE TABLE user_xiuxian (user_id TEXT PRIMARY KEY, stone INTEGER)")
            uow.execute("INSERT INTO user_xiuxian VALUES (?, ?)", ("user", 1000))
            apply_platform_schema(uow)
            apply_dufang_bet_payout(uow)
            apply_dufang_resolution(uow)
        with DatabaseUnitOfWork(self.player) as uow:
            apply_dufang_share_player(uow)
            apply_dufang_player_receipts(uow)
        self.application = DufangApplication(self.game, self.player)

    def tearDown(self) -> None:
        self.temp.cleanup()

    def test_bet_application_owns_transaction_and_replay(self) -> None:
        planned = self.application.plan_for_bet("bet-1", draw=lambda: WIN_PLAN)
        first = self.application.bet(
            operation_id="bet-1",
            user_id="user",
            cost=300,
            placed_at="2026-09-17 00:00:00",
            resolution=planned.resolution,
        )
        duplicate = self.application.bet(
            operation_id="bet-1",
            user_id="user",
            cost=300,
            placed_at="later",
            resolution=planned.resolution,
        )

        self.assertTrue(first.ok)
        self.assertTrue(duplicate.replayed)
        self.assertEqual(first.data["status"], "applied")
        with db_backend.connection(self.game) as conn:
            self.assertEqual(conn.execute("SELECT stone FROM user_xiuxian WHERE user_id=?", ("user",)).fetchone()[0], 700)
        with db_backend.connection(self.player) as conn:
            row = conn.execute("SELECT count,total_cost FROM unseal_data WHERE user_id=?", ("user",)).fetchone()
        self.assertEqual(tuple(row), (1, 300))

    def test_payout_application_owns_transaction_and_replay(self) -> None:
        planned = self.application.plan_for_bet("bet-for-payout", draw=lambda: WIN_PLAN)
        bet = self.application.bet(
            operation_id="bet-for-payout",
            user_id="user",
            cost=300,
            placed_at="2026-09-17 00:00:00",
            resolution=planned.resolution,
        )
        first = self.application.payout(
            operation_id="pay-1",
            user_id="user",
            bet_id=bet.data["bet_id"],
            settled_at="2026-09-17 00:01:00",
        )
        duplicate = self.application.payout(
            operation_id="pay-1",
            user_id="user",
            bet_id=bet.data["bet_id"],
            settled_at="later",
        )

        self.assertTrue(first.ok)
        self.assertTrue(duplicate.replayed)
        self.assertEqual(first.data["status"], "applied")
        with db_backend.connection(self.game) as conn:
            self.assertEqual(tuple(conn.execute("SELECT user_xiuxian.stone,dufang_bets.status FROM user_xiuxian JOIN dufang_bets ON user_xiuxian.user_id=dufang_bets.user_id WHERE user_xiuxian.user_id=?", ("user",)).fetchone()), (1300, "win"))
        with db_backend.connection(self.player) as conn:
            row = conn.execute("SELECT COALESCE(profit,0),COALESCE(loss,0) FROM unseal_data WHERE user_id=?", ("user",)).fetchone()
        self.assertEqual(tuple(row), (600, 0))


if __name__ == "__main__":
    unittest.main()
