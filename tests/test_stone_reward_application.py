from __future__ import annotations

import sqlite3
import tempfile
import unittest
from pathlib import Path

from nonebot_plugin_xiuxian_2.features.back.migrations import apply_stone_reward
from nonebot_plugin_xiuxian_2.features.back.stone_reward_application import StoneRewardApplication
from nonebot_plugin_xiuxian_2.infrastructure.database import DatabaseUnitOfWork
from tests.test_db_backend import db_backend


class StoneRewardApplicationTests(unittest.TestCase):
    def setUp(self) -> None:
        self.temp_dir = tempfile.TemporaryDirectory()
        self.database = Path(self.temp_dir.name) / "stone-reward.sqlite3"
        with db_backend.transaction(self.database) as conn:
            conn.execute(
                "CREATE TABLE user_xiuxian (user_id TEXT PRIMARY KEY, stone INTEGER NOT NULL)"
            )
            conn.execute(
                "CREATE TABLE back (user_id TEXT, goods_id INTEGER, goods_num INTEGER, "
                "bind_num INTEGER, UNIQUE(user_id,goods_id))"
            )
            conn.execute("INSERT INTO user_xiuxian VALUES (%s,%s)", ("user", 100))
            conn.execute("INSERT INTO back VALUES (%s,%s,%s,%s)", ("user", 20020, 3, 3))
        with DatabaseUnitOfWork(self.database) as uow:
            apply_stone_reward(uow)
        self.application = StoneRewardApplication(self.database)

    def tearDown(self) -> None:
        self.temp_dir.cleanup()

    def scalar(self, sql: str, params=()):
        with db_backend.connection(self.database) as conn:
            return conn.execute(sql, params).fetchone()[0]

    def test_applies_fixed_rewards_and_replays_original_payload(self) -> None:
        first = self.application.apply(
            "stone-1", "user", reward_type="spirit_stone_bag", item_id=20020, rewards=(10, 20)
        )
        duplicate = self.application.apply(
            "stone-1", "user", reward_type="tianji_stone_trigger", item_id=20020, rewards=(99, 99)
        )

        self.assertEqual((first.status, first.rewards, first.total_stone), ("applied", (10, 20), 30))
        self.assertEqual((duplicate.status, duplicate.rewards, duplicate.reward_type), ("duplicate", (10, 20), "spirit_stone_bag"))
        self.assertEqual((self.scalar("SELECT stone FROM user_xiuxian"), self.scalar("SELECT goods_num FROM back")), (130, 1))
        self.assertEqual(self.scalar("SELECT bind_num FROM back"), 1)

    def test_insufficient_items_do_not_grant_stone(self) -> None:
        result = self.application.apply(
            "stone-short", "user", reward_type="spirit_stone_bag", item_id=20020, rewards=(1, 2, 3, 4)
        )
        self.assertEqual(result.status, "item_insufficient")
        self.assertEqual(self.scalar("SELECT stone FROM user_xiuxian"), 100)

    def test_trigger_failure_rolls_back_item_and_stone(self) -> None:
        with db_backend.transaction(self.database) as conn:
            conn.execute(
                "CREATE TRIGGER fail_stone_reward BEFORE INSERT ON stone_item_reward_operations "
                "BEGIN SELECT RAISE(ABORT, 'operation failed'); END"
            )
        with self.assertRaises(db_backend.IntegrityError):
            self.application.apply(
                "stone-fail", "user", reward_type="spirit_stone_bag", item_id=20020, rewards=(10, 20)
            )
        self.assertEqual(self.scalar("SELECT stone FROM user_xiuxian"), 100)
        self.assertEqual(self.scalar("SELECT goods_num FROM back"), 3)

    def test_missing_schema_is_not_created_at_request_time(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            database = Path(directory) / "empty.sqlite3"
            with DatabaseUnitOfWork(database) as uow:
                uow.execute("CREATE TABLE user_xiuxian(user_id TEXT PRIMARY KEY, stone INTEGER)")
                uow.execute("CREATE TABLE back(user_id TEXT, goods_id INTEGER, goods_num INTEGER)")
            with self.assertRaises(sqlite3.OperationalError):
                StoneRewardApplication(database).apply(
                    "missing-schema", "user", reward_type="spirit_stone_bag", item_id=20020, rewards=(1,)
                )


if __name__ == "__main__":
    unittest.main()
