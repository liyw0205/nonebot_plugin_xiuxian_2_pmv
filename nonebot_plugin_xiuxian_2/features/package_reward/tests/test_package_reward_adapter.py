from __future__ import annotations

import tempfile
import unittest
from pathlib import Path

import nonebot

nonebot.init()

from ..domain import PackageReward
from ..application import PackageRewardApplication
from ....infrastructure.database import DatabaseUnitOfWork
from ....plugin import apply_platform_schema
from ..migrations import apply_package_reward
from ....xiuxian.xiuxian_back.package_reward_service import PackageRewardService


class PackageRewardAdapterTests(unittest.TestCase):
    def test_legacy_facade_forwards_to_application(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            database = Path(directory) / "game.db"
            with DatabaseUnitOfWork(database) as uow:
                uow.execute("CREATE TABLE user_xiuxian (user_id TEXT PRIMARY KEY, stone INTEGER NOT NULL)")
                uow.execute("CREATE TABLE back (user_id TEXT, goods_id INTEGER, goods_name TEXT, goods_type TEXT, goods_num INTEGER, bind_num INTEGER, UNIQUE(user_id, goods_id))")
                uow.execute("INSERT INTO user_xiuxian VALUES (?, ?)", ("u", 1))
                uow.execute("INSERT INTO back VALUES (?, ?, ?, ?, ?, ?)", ("u", 9, "礼包", "礼包", 1, 1))
                apply_platform_schema(uow)
                apply_package_reward(uow)
            result = PackageRewardService(database).apply("adapter-1", "u", 9, 1, (PackageReward(None, "灵石", None, 2),), max_goods_num=10)
            self.assertEqual(result.status, "applied")
            self.assertIsInstance(PackageRewardApplication(database), PackageRewardApplication)


if __name__ == "__main__":
    unittest.main()
