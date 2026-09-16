from __future__ import annotations

import tempfile
import unittest
from pathlib import Path

import nonebot

nonebot.init()

from nonebot_plugin_xiuxian_2.features.dufang.application import DufangApplication
from nonebot_plugin_xiuxian_2.infrastructure.database import DatabaseUnitOfWork
from nonebot_plugin_xiuxian_2.plugin import apply_platform_schema
from tests.test_db_backend import db_backend


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
        self.application = DufangApplication(self.game, self.player)

    def tearDown(self) -> None:
        self.temp.cleanup()

    def test_bet_application_owns_transaction_and_replay(self) -> None:
        first = self.application.bet(
            operation_id="bet-1",
            user_id="user",
            cost=300,
            placed_at="2026-09-17 00:00:00",
        )
        duplicate = self.application.bet(
            operation_id="bet-1",
            user_id="user",
            cost=300,
            placed_at="later",
        )

        self.assertTrue(first.ok)
        self.assertTrue(duplicate.replayed)
        self.assertEqual(first.data["status"], "applied")
        with db_backend.connection(self.game) as conn:
            self.assertEqual(conn.execute("SELECT stone FROM user_xiuxian WHERE user_id=?", ("user",)).fetchone()[0], 700)
        with db_backend.connection(self.player) as conn:
            row = conn.execute("SELECT count,total_cost FROM unseal_data WHERE user_id=?", ("user",)).fetchone()
        self.assertEqual(tuple(row), (1, 300))


if __name__ == "__main__":
    unittest.main()
