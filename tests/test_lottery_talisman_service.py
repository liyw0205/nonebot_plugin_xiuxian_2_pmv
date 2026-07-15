from __future__ import annotations

import tempfile
import unittest
from pathlib import Path

import nonebot

nonebot.init()

from nonebot_plugin_xiuxian_2.xiuxian.xiuxian_back.transaction_service import (
    LotteryReward,
    LotteryTalismanService,
)
from tests.test_db_backend import db_backend


class LotteryTalismanServiceTests(unittest.TestCase):
    def setUp(self) -> None:
        self.temp_dir = tempfile.TemporaryDirectory()
        self.database = Path(self.temp_dir.name) / "lottery-talisman.sqlite3"
        with db_backend.transaction(self.database) as conn:
            conn.execute(
                "CREATE TABLE back (user_id TEXT, goods_id INTEGER, goods_name TEXT, "
                "goods_type TEXT, goods_num INTEGER, bind_num INTEGER, "
                "UNIQUE(user_id, goods_id))"
            )
            conn.execute(
                "INSERT INTO back VALUES (%s, %s, %s, %s, %s, %s)",
                ("user", 20010, "灵签宝箓", "消耗品", 3, 2),
            )
        self.service = LotteryTalismanService(self.database)

    def tearDown(self) -> None:
        self.temp_dir.cleanup()

    def inventory(self, goods_id: int):
        with db_backend.connection(self.database) as conn:
            row = conn.execute(
                "SELECT goods_num, bind_num FROM back WHERE user_id=%s AND goods_id=%s",
                ("user", goods_id),
            ).fetchone()
        return None if row is None else tuple(map(int, row))

    def operation_count(self) -> int:
        with db_backend.connection(self.database) as conn:
            exists = conn.execute(
                "SELECT 1 FROM sqlite_master WHERE type='table' AND name=%s",
                ("lottery_talisman_operations",),
            ).fetchone()
            if not exists:
                return 0
            return int(conn.execute("SELECT COUNT(*) FROM lottery_talisman_operations").fetchone()[0])

    def test_consumes_talismans_and_grants_rewards_atomically(self) -> None:
        rewards = (
            LotteryReward(9001, "青锋剑", "法器", 1),
            LotteryReward(9001, "青锋剑", "法器", 1),
            LotteryReward(9002, "玄铁甲", "防具", 1),
        )
        result = self.service.apply("lottery-1", "user", 20010, 2, rewards, max_goods_num=1000)
        self.assertEqual(result.status, "applied")
        self.assertEqual(self.inventory(20010), (1, 1))
        self.assertEqual(self.inventory(9001), (2, 2))
        self.assertEqual(self.inventory(9002), (1, 1))
        self.assertEqual(self.operation_count(), 1)

    def test_empty_reward_roll_still_consumes_requested_quantity(self) -> None:
        result = self.service.apply("lottery-empty", "user", 20010, 2, (), max_goods_num=1000)
        self.assertEqual((result.status, result.rewards), ("applied", ()))
        self.assertEqual(self.inventory(20010), (1, 1))
        self.assertEqual(self.operation_count(), 1)

    def test_duplicate_reuses_first_roll_without_consuming_or_granting_twice(self) -> None:
        first_reward = (LotteryReward(9001, "青锋剑", "法器", 1),)
        second_reward = (LotteryReward(9002, "玄铁甲", "防具", 3),)
        first = self.service.apply("lottery-repeat", "user", 20010, 1, first_reward, max_goods_num=1000)
        second = self.service.apply("lottery-repeat", "user", 20010, 3, second_reward, max_goods_num=1000)
        self.assertEqual((first.status, second.status), ("applied", "duplicate"))
        self.assertEqual((second.quantity, second.rewards), (1, first_reward))
        self.assertEqual(self.inventory(20010), (2, 2))
        self.assertEqual(self.inventory(9001), (1, 1))
        self.assertIsNone(self.inventory(9002))
        self.assertEqual(self.operation_count(), 1)

    def test_insufficient_talismans_do_not_grant_rewards(self) -> None:
        result = self.service.apply(
            "lottery-short", "user", 20010, 4,
            (LotteryReward(9001, "青锋剑", "法器", 1),), max_goods_num=1000,
        )
        self.assertEqual(result.status, "item_insufficient")
        self.assertEqual(self.inventory(20010), (3, 2))
        self.assertIsNone(self.inventory(9001))
        self.assertEqual(self.operation_count(), 0)

    def test_operation_failure_rolls_back_consumption_and_rewards(self) -> None:
        with db_backend.transaction(self.database) as conn:
            self.service._ensure_operations(conn)
            conn.execute(
                "CREATE TRIGGER fail_lottery BEFORE INSERT ON lottery_talisman_operations "
                "BEGIN SELECT RAISE(ABORT, 'operation failed'); END"
            )
        with self.assertRaises(db_backend.IntegrityError):
            self.service.apply(
                "lottery-fail", "user", 20010, 1,
                (LotteryReward(9001, "青锋剑", "法器", 1),), max_goods_num=1000,
            )
        self.assertEqual(self.inventory(20010), (3, 2))
        self.assertIsNone(self.inventory(9001))
        self.assertEqual(self.operation_count(), 0)

    def test_existing_inventory_reward_is_merged_and_bound_count_capped(self) -> None:
        with db_backend.transaction(self.database) as conn:
            conn.execute(
                "INSERT INTO back VALUES (%s, %s, %s, %s, %s, %s)",
                ("user", 9001, "青锋剑", "法器", 2, 1),
            )

        result = self.service.apply(
            "lottery-merge",
            "user",
            20010,
            1,
            (LotteryReward(9001, "青锋剑", "法器", 2),),
            max_goods_num=1000,
        )

        self.assertEqual(result.status, "applied")
        self.assertEqual(self.inventory(20010), (2, 2))
        self.assertEqual(self.inventory(9001), (4, 3))
        self.assertEqual(self.operation_count(), 1)

    def test_reward_quantity_is_capped_by_max_goods_num(self) -> None:
        with db_backend.transaction(self.database) as conn:
            conn.execute(
                "INSERT INTO back VALUES (%s, %s, %s, %s, %s, %s)",
                ("user", 9001, "青锋剑", "法器", 4, 4),
            )

        result = self.service.apply(
            "lottery-cap",
            "user",
            20010,
            1,
            (LotteryReward(9001, "青锋剑", "法器", 3),),
            max_goods_num=5,
        )

        self.assertEqual(result.status, "applied")
        self.assertEqual(self.inventory(20010), (2, 2))
        self.assertEqual(self.inventory(9001), (5, 5))
        self.assertEqual(self.operation_count(), 1)


if __name__ == "__main__":
    unittest.main()
