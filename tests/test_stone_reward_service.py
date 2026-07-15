from __future__ import annotations

import tempfile
import unittest
from pathlib import Path

import nonebot

nonebot.init()

from nonebot_plugin_xiuxian_2.xiuxian.xiuxian_back.transaction_service import (
    StoneItemRewardService,
)
from tests.test_db_backend import db_backend


class StoneItemRewardServiceTests(unittest.TestCase):
    def setUp(self) -> None:
        self.temp_dir = tempfile.TemporaryDirectory()
        self.database = Path(self.temp_dir.name) / "xiuxian.sqlite3"
        with db_backend.transaction(self.database) as conn:
            conn.execute(
                "CREATE TABLE user_xiuxian (user_id TEXT PRIMARY KEY, stone INTEGER NOT NULL)"
            )
            conn.execute(
                """
                CREATE TABLE back (
                    user_id TEXT NOT NULL, goods_id INTEGER NOT NULL,
                    goods_num INTEGER NOT NULL, bind_num INTEGER DEFAULT 0,
                    UNIQUE (user_id, goods_id)
                )
                """
            )
            conn.execute("INSERT INTO user_xiuxian VALUES (%s, %s)", ("user-1", 100))
            conn.execute("INSERT INTO back VALUES (%s, %s, %s, %s)", ("user-1", 20020, 3, 3))
        self.service = StoneItemRewardService(self.database)

    def tearDown(self) -> None:
        self.temp_dir.cleanup()

    def scalar(self, sql: str):
        with db_backend.connection(self.database) as conn:
            return conn.execute(sql).fetchone()[0]

    def apply(self, operation_id="stone-bag-1", rewards=(10, 20)):
        return self.service.apply(
            operation_id,
            "user-1",
            reward_type="spirit_stone_bag",
            item_id=20020,
            rewards=rewards,
        )

    def test_applies_fixed_rewards_and_consumes_items(self) -> None:
        result = self.apply()

        self.assertEqual(result.status, "applied")
        self.assertEqual(result.rewards, (10, 20))
        self.assertEqual(result.total_stone, 30)
        self.assertEqual(self.scalar("SELECT stone FROM user_xiuxian"), 130)
        self.assertEqual(self.scalar("SELECT goods_num FROM back"), 1)
        self.assertEqual(self.scalar("SELECT bind_num FROM back"), 1)

    def test_duplicate_returns_original_rewards_without_mutation(self) -> None:
        first = self.apply()
        second = self.apply(rewards=(99, 99))

        self.assertEqual(first.status, "applied")
        self.assertEqual(second.status, "duplicate")
        self.assertEqual(second.rewards, (10, 20))
        self.assertEqual(self.scalar("SELECT stone FROM user_xiuxian"), 130)
        self.assertEqual(self.scalar("SELECT goods_num FROM back"), 1)

    def test_insufficient_items_do_not_grant_stone(self) -> None:
        result = self.apply(rewards=(10, 20, 30, 40))

        self.assertEqual(result.status, "item_insufficient")
        self.assertEqual(self.scalar("SELECT stone FROM user_xiuxian"), 100)
        self.assertEqual(self.scalar("SELECT goods_num FROM back"), 3)

    def test_operation_failure_rolls_back_item_and_stone(self) -> None:
        with db_backend.transaction(self.database) as conn:
            self.service._ensure_operations(conn)
            conn.execute(
                """
                CREATE TRIGGER fail_reward_operation
                BEFORE INSERT ON stone_item_reward_operations
                BEGIN SELECT RAISE(ABORT, 'operation failed'); END
                """
            )

        with self.assertRaises(db_backend.IntegrityError):
            self.apply()

        self.assertEqual(self.scalar("SELECT stone FROM user_xiuxian"), 100)
        self.assertEqual(self.scalar("SELECT goods_num FROM back"), 3)

    def test_tianji_reward_uses_same_transaction_boundary(self) -> None:
        with db_backend.transaction(self.database) as conn:
            conn.execute(
                "INSERT INTO back VALUES (%s, %s, %s, %s)",
                ("user-1", 20021, 2, 0),
            )

        result = self.service.apply(
            "tianji-1",
            "user-1",
            reward_type="tianji_stone_trigger",
            item_id=20021,
            rewards=(10_000_001, 99_999_999),
        )
        duplicate = self.service.apply(
            "tianji-1",
            "user-1",
            reward_type="tianji_stone_trigger",
            item_id=20021,
            rewards=(10_000_000, 10_000_000),
        )

        self.assertEqual(result.status, "applied")
        self.assertEqual(duplicate.status, "duplicate")
        self.assertEqual(duplicate.rewards, (10_000_001, 99_999_999))
        self.assertEqual(self.scalar("SELECT stone FROM user_xiuxian"), 110_000_100)
        self.assertEqual(
            self.scalar("SELECT goods_num FROM back WHERE goods_id=20021"), 0
        )


if __name__ == "__main__":
    unittest.main()
