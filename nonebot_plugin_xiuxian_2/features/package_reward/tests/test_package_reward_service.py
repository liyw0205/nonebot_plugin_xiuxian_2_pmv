from __future__ import annotations

import tempfile
import unittest
from pathlib import Path

from ....infrastructure.database import DatabaseUnitOfWork
from ....plugin import apply_platform_schema
from ..application import PackageRewardApplication
from ..domain import PackageReward
from ..migrations import apply_package_reward


class PackageRewardApplicationTests(unittest.TestCase):
    def setUp(self) -> None:
        self.temp_dir = tempfile.TemporaryDirectory()
        self.database = Path(self.temp_dir.name) / "game.db"
        with DatabaseUnitOfWork(self.database) as uow:
            uow.execute("CREATE TABLE user_xiuxian (user_id TEXT PRIMARY KEY, stone INTEGER NOT NULL)")
            uow.execute(
                "CREATE TABLE back (user_id TEXT NOT NULL, goods_id INTEGER NOT NULL, goods_name TEXT, goods_type TEXT, goods_num INTEGER NOT NULL, bind_num INTEGER DEFAULT 0, UNIQUE(user_id, goods_id))"
            )
            uow.execute("INSERT INTO user_xiuxian VALUES (?, ?)", ("user", 100))
            uow.execute("INSERT INTO back VALUES (?, ?, ?, ?, ?, ?)", ("user", 3001, "礼包", "礼包", 2, 2))
            apply_platform_schema(uow)
            apply_package_reward(uow)
        self.application = PackageRewardApplication(self.database)
        self.rewards = (PackageReward(None, "灵石", None, 50), PackageReward(4001, "丹药", "丹药", 2))

    def tearDown(self) -> None:
        self.temp_dir.cleanup()

    def test_apply_and_replay_use_operation_ledger(self) -> None:
        first = self.application.open_package(operation_id="pkg-1", user_id="user", package_id=3001, quantity=1, rewards=self.rewards, max_goods_num=1000)
        second = self.application.open_package(operation_id="pkg-1", user_id="user", package_id=3001, quantity=1, rewards=self.rewards, max_goods_num=1000)
        self.assertEqual((first.status, second.status), ("applied", "replayed"))
        self.assertEqual(second.data, first.data)
        with DatabaseUnitOfWork(self.database) as uow:
            self.assertEqual(uow.query_one("SELECT stone FROM user_xiuxian WHERE user_id = ?", ("user",))["stone"], 150)
            self.assertEqual(uow.query_one("SELECT goods_num FROM back WHERE goods_id = ?", (3001,))["goods_num"], 1)
            self.assertEqual(uow.query_one("SELECT goods_num FROM back WHERE goods_id = ?", (4001,))["goods_num"], 2)

    def test_rejection_does_not_consume_package(self) -> None:
        result = self.application.open_package(operation_id="pkg-2", user_id="user", package_id=3001, quantity=3, rewards=self.rewards, max_goods_num=1000)
        self.assertEqual((result.status, result.code), ("rejected", "item_insufficient"))
        with DatabaseUnitOfWork(self.database) as uow:
            self.assertEqual(uow.query_one("SELECT goods_num FROM back WHERE goods_id = ?", (3001,))["goods_num"], 2)

    def test_operation_hash_conflict_is_rejected(self) -> None:
        self.application.open_package(operation_id="pkg-3", user_id="user", package_id=3001, quantity=1, rewards=self.rewards, max_goods_num=1000)
        with self.assertRaises(Exception):
            self.application.open_package(operation_id="pkg-3", user_id="user", package_id=3001, quantity=2, rewards=self.rewards, max_goods_num=1000)

    def test_capacity_rejection_rolls_back_package_and_stone(self) -> None:
        with DatabaseUnitOfWork(self.database) as uow:
            uow.execute("INSERT INTO back VALUES (?, ?, ?, ?, ?, ?)", ("user", 4001, "丹药", "丹药", 999, 999))
        result = self.application.open_package(operation_id="pkg-4", user_id="user", package_id=3001, quantity=1, rewards=self.rewards, max_goods_num=1000)
        self.assertEqual(result.code, "inventory_full")
        with DatabaseUnitOfWork(self.database) as uow:
            self.assertEqual(uow.query_one("SELECT stone FROM user_xiuxian WHERE user_id = ?", ("user",))["stone"], 100)
            self.assertEqual(uow.query_one("SELECT goods_num FROM back WHERE goods_id = ?", (3001,))["goods_num"], 2)

    def test_negative_stone_rejection_rolls_back_package(self) -> None:
        result = self.application.open_package(operation_id="pkg-5", user_id="user", package_id=3001, quantity=1, rewards=(PackageReward(None, "灵石", None, -101),), max_goods_num=1000)
        self.assertEqual(result.code, "stone_insufficient")
        with DatabaseUnitOfWork(self.database) as uow:
            self.assertEqual(uow.query_one("SELECT stone FROM user_xiuxian WHERE user_id = ?", ("user",))["stone"], 100)
            self.assertEqual(uow.query_one("SELECT goods_num FROM back WHERE goods_id = ?", (3001,))["goods_num"], 2)


if __name__ == "__main__":
    unittest.main()
