from __future__ import annotations

import tempfile
import unittest
from pathlib import Path

import nonebot

nonebot.init()

from nonebot_plugin_xiuxian_2.xiuxian.xiuxian_back.transaction_service import (
    PackageReward,
    PackageRewardService,
)
from tests.test_db_backend import db_backend


class PackageRewardServiceTests(unittest.TestCase):
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
                    goods_name TEXT, goods_type TEXT, goods_num INTEGER NOT NULL,
                    bind_num INTEGER DEFAULT 0,
                    UNIQUE (user_id, goods_id)
                )
                """
            )
            conn.execute("INSERT INTO user_xiuxian VALUES (%s, %s)", ("user", 100))
            conn.execute(
                "INSERT INTO back VALUES (%s, %s, %s, %s, %s, %s)",
                ("user", 3001, "测试礼包", "礼包", 3, 3),
            )
        self.service = PackageRewardService(self.database)
        self.rewards = (
            PackageReward(None, "灵石", None, 50),
            PackageReward(4001, "测试丹药", "丹药", 2),
        )

    def tearDown(self) -> None:
        self.temp_dir.cleanup()

    def scalar(self, sql, params=()):
        with db_backend.connection(self.database) as conn:
            row = conn.execute(sql, params).fetchone()
            return row[0] if row else None

    def apply(self, operation_id="package-1", quantity=2, rewards=None):
        return self.service.apply(
            operation_id,
            "user",
            3001,
            quantity,
            self.rewards if rewards is None else rewards,
            max_goods_num=1000,
        )

    def test_consumes_packages_and_grants_all_rewards_atomically(self) -> None:
        result = self.apply()

        self.assertEqual(result.status, "applied")
        self.assertEqual(self.scalar("SELECT stone FROM user_xiuxian"), 150)
        self.assertEqual(self.scalar("SELECT goods_num FROM back WHERE goods_id=3001"), 1)
        self.assertEqual(self.scalar("SELECT bind_num FROM back WHERE goods_id=3001"), 1)
        self.assertEqual(self.scalar("SELECT goods_num FROM back WHERE goods_id=4001"), 2)

    def test_duplicate_reuses_fixed_rewards_without_second_mutation(self) -> None:
        first = self.apply("package-repeat")
        second = self.apply(
            "package-repeat",
            rewards=(PackageReward(None, "灵石", None, 999),),
        )

        self.assertEqual((first.status, second.status), ("applied", "duplicate"))
        self.assertEqual(second.rewards, self.rewards)
        self.assertEqual(self.scalar("SELECT stone FROM user_xiuxian"), 150)
        self.assertEqual(self.scalar("SELECT goods_num FROM back WHERE goods_id=3001"), 1)

    def test_same_operation_with_different_quantity_is_rejected(self) -> None:
        first = self.apply("package-conflict", quantity=1)
        conflict = self.apply("package-conflict", quantity=2)

        self.assertEqual((first.status, conflict.status), ("applied", "state_changed"))
        self.assertEqual(self.scalar("SELECT stone FROM user_xiuxian"), 150)
        self.assertEqual(self.scalar("SELECT goods_num FROM back WHERE goods_id=3001"), 2)

    def test_insufficient_packages_do_not_grant_rewards(self) -> None:
        result = self.apply(quantity=4)

        self.assertEqual(result.status, "item_insufficient")
        self.assertEqual(self.scalar("SELECT stone FROM user_xiuxian"), 100)
        self.assertEqual(self.scalar("SELECT goods_num FROM back WHERE goods_id=3001"), 3)
        self.assertIsNone(self.scalar("SELECT goods_num FROM back WHERE goods_id=4001"))

    def test_negative_stone_reward_requires_sufficient_balance(self) -> None:
        result = self.apply(
            rewards=(PackageReward(None, "灵石", None, -101),)
        )

        self.assertEqual(result.status, "stone_insufficient")
        self.assertEqual(self.scalar("SELECT stone FROM user_xiuxian"), 100)
        self.assertEqual(self.scalar("SELECT goods_num FROM back WHERE goods_id=3001"), 3)

    def test_full_reward_stack_rolls_back_package_and_stone(self) -> None:
        with db_backend.transaction(self.database) as conn:
            conn.execute(
                "INSERT INTO back VALUES (%s, %s, %s, %s, %s, %s)",
                ("user", 4001, "测试丹药", "丹药", 999, 999),
            )

        result = self.apply()

        self.assertEqual(result.status, "inventory_full")
        self.assertEqual(self.scalar("SELECT stone FROM user_xiuxian"), 100)
        self.assertEqual(self.scalar("SELECT goods_num FROM back WHERE goods_id=3001"), 3)
        self.assertEqual(self.scalar("SELECT goods_num FROM back WHERE goods_id=4001"), 999)

    def test_missing_user_does_not_consume_package_or_grant_item(self) -> None:
        with db_backend.transaction(self.database) as conn:
            conn.execute("DELETE FROM user_xiuxian")

        result = self.apply(rewards=(PackageReward(4001, "测试丹药", "丹药", 2),))

        self.assertEqual(result.status, "user_missing")
        self.assertEqual(self.scalar("SELECT goods_num FROM back WHERE goods_id=3001"), 3)
        self.assertIsNone(self.scalar("SELECT goods_num FROM back WHERE goods_id=4001"))

    def test_operation_insert_failure_rolls_back_package_and_rewards(self) -> None:
        with db_backend.transaction(self.database) as conn:
            self.service._ensure_operations(conn)
            conn.execute(
                "CREATE TRIGGER fail_package_operation "
                "BEFORE INSERT ON package_reward_operations "
                "BEGIN SELECT RAISE(ABORT, 'operation failed'); END"
            )

        with self.assertRaises(db_backend.IntegrityError):
            self.apply("package-fail")

        self.assertEqual(self.scalar("SELECT stone FROM user_xiuxian"), 100)
        self.assertEqual(self.scalar("SELECT goods_num FROM back WHERE goods_id=3001"), 3)
        self.assertIsNone(self.scalar("SELECT goods_num FROM back WHERE goods_id=4001"))


if __name__ == "__main__":
    unittest.main()
