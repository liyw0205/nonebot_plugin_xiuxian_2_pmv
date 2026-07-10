from __future__ import annotations

import tempfile
import unittest
from pathlib import Path

import nonebot

nonebot.init()

from nonebot_plugin_xiuxian_2.xiuxian.xiuxian_puppet.operation_service import (
    PuppetOperationService,
)
from tests.test_db_backend import db_backend


class PuppetOperationServiceTests(unittest.TestCase):
    def setUp(self) -> None:
        self.temp_dir = tempfile.TemporaryDirectory()
        root = Path(self.temp_dir.name)
        self.game_database = root / "xiuxian.sqlite3"
        self.player_database = root / "player.sqlite3"
        with db_backend.transaction(self.game_database) as conn:
            conn.execute(
                """
                CREATE TABLE user_xiuxian (
                    user_id TEXT PRIMARY KEY,
                    stone INTEGER NOT NULL,
                    blessed_spot_flag INTEGER NOT NULL
                )
                """
            )
            conn.execute(
                "INSERT INTO user_xiuxian VALUES (%s, %s, %s)",
                ("user-1", 200, 1),
            )
        with db_backend.transaction(self.player_database) as conn:
            conn.execute(
                """
                CREATE TABLE mix_elixir_info (
                    user_id TEXT PRIMARY KEY,
                    "灵田傀儡" TEXT NOT NULL
                )
                """
            )
            conn.execute(
                "INSERT INTO mix_elixir_info VALUES (%s, %s)", ("user-1", "0")
            )
        self.service = PuppetOperationService(self.game_database, self.player_database)

    def tearDown(self) -> None:
        self.temp_dir.cleanup()

    def scalar(self, database: Path, sql: str, params=()):
        with db_backend.connection(database) as conn:
            row = conn.execute(sql, params).fetchone()
            return row[0] if row else None

    def test_purchase_deducts_stone_and_sets_level_together(self) -> None:
        result = self.service.purchase("puppet-buy-1", "user-1", 50)

        self.assertTrue(result.applied)
        self.assertEqual(result.current_level, 1)
        self.assertEqual(result.stone_cost, 50)
        self.assertEqual(
            self.scalar(
                self.game_database,
                "SELECT stone FROM user_xiuxian WHERE user_id=%s",
                ("user-1",),
            ),
            150,
        )
        self.assertEqual(
            self.scalar(
                self.player_database,
                'SELECT "灵田傀儡" FROM mix_elixir_info WHERE user_id=%s',
                ("user-1",),
            ),
            "1",
        )

    def test_repeated_purchase_operation_does_not_charge_twice(self) -> None:
        first = self.service.purchase("puppet-buy-repeat", "user-1", 50)
        second = self.service.purchase("puppet-buy-repeat", "user-1", 50)

        self.assertTrue(first.applied)
        self.assertEqual(second.status, "duplicate")
        self.assertEqual(second.current_level, 1)
        self.assertEqual(
            self.scalar(
                self.game_database,
                "SELECT stone FROM user_xiuxian WHERE user_id=%s",
                ("user-1",),
            ),
            150,
        )

    def test_insufficient_stone_leaves_purchase_state_unchanged(self) -> None:
        result = self.service.purchase("puppet-buy-poor", "user-1", 300)

        self.assertEqual(result.status, "stone_insufficient")
        self.assertEqual(
            self.scalar(
                self.game_database,
                "SELECT stone FROM user_xiuxian WHERE user_id=%s",
                ("user-1",),
            ),
            200,
        )
        self.assertEqual(
            self.scalar(
                self.player_database,
                'SELECT "灵田傀儡" FROM mix_elixir_info WHERE user_id=%s',
                ("user-1",),
            ),
            "0",
        )

    def test_upgrade_is_atomic_and_repeated_operation_is_idempotent(self) -> None:
        self.service.purchase("puppet-buy-upgrade", "user-1", 50)
        costs = {1: 60, 2: 90, 3: 0}

        first = self.service.upgrade(
            "puppet-upgrade-1", "user-1", costs, max_level=3
        )
        second = self.service.upgrade(
            "puppet-upgrade-1", "user-1", costs, max_level=3
        )

        self.assertTrue(first.applied)
        self.assertEqual(first.current_level, 2)
        self.assertEqual(second.status, "duplicate")
        self.assertEqual(second.current_level, 2)
        self.assertEqual(
            self.scalar(
                self.game_database,
                "SELECT stone FROM user_xiuxian WHERE user_id=%s",
                ("user-1",),
            ),
            90,
        )
        self.assertEqual(
            self.scalar(
                self.player_database,
                'SELECT "灵田傀儡" FROM mix_elixir_info WHERE user_id=%s',
                ("user-1",),
            ),
            "2",
        )

    def test_player_write_failure_rolls_back_stone_and_operation(self) -> None:
        with db_backend.transaction(self.player_database) as conn:
            conn.execute(
                """
                CREATE TRIGGER fail_puppet_update
                BEFORE UPDATE OF "灵田傀儡" ON mix_elixir_info
                BEGIN SELECT RAISE(ABORT, 'puppet update failed'); END
                """
            )

        with self.assertRaises(db_backend.IntegrityError):
            self.service.purchase("puppet-buy-failure", "user-1", 50)

        self.assertEqual(
            self.scalar(
                self.game_database,
                "SELECT stone FROM user_xiuxian WHERE user_id=%s",
                ("user-1",),
            ),
            200,
        )
        self.assertEqual(
            self.scalar(
                self.player_database,
                'SELECT "灵田傀儡" FROM mix_elixir_info WHERE user_id=%s',
                ("user-1",),
            ),
            "0",
        )
        with db_backend.connection(self.game_database) as conn:
            self.assertFalse(conn.table_exists("puppet_operations"))


if __name__ == "__main__":
    unittest.main()
