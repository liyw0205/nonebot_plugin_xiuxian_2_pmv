from __future__ import annotations

import sqlite3
import tempfile
import unittest
from pathlib import Path

from nonebot_plugin_xiuxian_2.features.back.lottery_talisman_application import LotteryReward, LotteryTalismanApplication
from nonebot_plugin_xiuxian_2.features.back.migrations import apply_lottery_talisman
from nonebot_plugin_xiuxian_2.infrastructure.database import DatabaseUnitOfWork
from tests.test_db_backend import db_backend


class LotteryTalismanApplicationTests(unittest.TestCase):
    def setUp(self) -> None:
        self.temp_dir = tempfile.TemporaryDirectory()
        self.database = Path(self.temp_dir.name) / "lottery.sqlite3"
        with db_backend.transaction(self.database) as conn:
            conn.execute(
                "CREATE TABLE back (user_id TEXT, goods_id INTEGER, goods_name TEXT, goods_type TEXT, "
                "goods_num INTEGER, bind_num INTEGER, UNIQUE(user_id,goods_id))"
            )
            conn.execute(
                "INSERT INTO back VALUES (%s,%s,%s,%s,%s,%s)",
                ("user", 20010, "灵签宝箓", "消耗品", 3, 2),
            )
        with DatabaseUnitOfWork(self.database) as uow:
            apply_lottery_talisman(uow)
        self.application = LotteryTalismanApplication(self.database)

    def tearDown(self) -> None:
        self.temp_dir.cleanup()

    def _state(self):
        with db_backend.connection(self.database) as conn:
            rows = conn.execute(
                "SELECT goods_id,goods_num,bind_num FROM back ORDER BY goods_id"
            ).fetchall()
            count = conn.execute("SELECT COUNT(*) FROM lottery_talisman_operations").fetchone()[0]
        return [tuple(map(int, row)) for row in rows], int(count)

    def test_success_duplicate_and_reward_merge(self) -> None:
        rewards = (
            LotteryReward(9001, "青锋剑", "法器", 1),
            LotteryReward(9001, "青锋剑", "法器", 1),
            LotteryReward(9002, "玄铁甲", "防具", 1),
        )
        first = self.application.apply("lottery-1", "user", 20010, 2, rewards, max_goods_num=1000)
        duplicate = self.application.apply(
            "lottery-1", "user", 20010, 1,
            (LotteryReward(9002, "玄铁甲", "防具", 3),), max_goods_num=1000,
        )
        self.assertEqual((first.status, duplicate.status, duplicate.rewards), ("applied", "duplicate", rewards))
        self.assertEqual(([(9001, 2, 2), (9002, 1, 1), (20010, 1, 1)], 1), self._state())

    def test_empty_rewards_still_consume_talismans(self) -> None:
        result = self.application.apply("lottery-empty", "user", 20010, 2, (), max_goods_num=1000)
        self.assertEqual((result.status, result.rewards), ("applied", ()))
        self.assertEqual(([(20010, 1, 1)], 1), self._state())

    def test_missing_schema_is_not_created_at_request_time(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            database = Path(directory) / "empty.sqlite3"
            with DatabaseUnitOfWork(database) as uow:
                uow.execute("CREATE TABLE back(user_id TEXT, goods_id INTEGER, goods_num INTEGER)")
            with self.assertRaises(sqlite3.OperationalError):
                LotteryTalismanApplication(database).apply(
                    "missing-schema", "user", 20010, 1, (), max_goods_num=1000
                )


if __name__ == "__main__":
    unittest.main()
