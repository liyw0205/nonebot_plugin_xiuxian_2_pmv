import tempfile
import unittest
from pathlib import Path

from nonebot_plugin_xiuxian_2.features.back.application import BackApplication
from nonebot_plugin_xiuxian_2.features.package_reward.application import PackageRewardApplication
from nonebot_plugin_xiuxian_2.features.package_reward.domain import PackageReward
from nonebot_plugin_xiuxian_2.features.package_reward.migrations import apply_package_reward
from nonebot_plugin_xiuxian_2.infrastructure.database import DatabaseUnitOfWork
from nonebot_plugin_xiuxian_2.plugin import apply_platform_schema
from tests.test_db_backend import db_backend


class BackApplicationPackageTests(unittest.TestCase):
    def test_default_open_package_uses_package_reward_application(self):
        with tempfile.TemporaryDirectory() as temp:
            database = Path(temp) / "game.db"
            with db_backend.transaction(database) as conn:
                conn.execute("CREATE TABLE user_xiuxian(user_id TEXT PRIMARY KEY, stone INTEGER)")
                conn.execute("INSERT INTO user_xiuxian VALUES('u',100)")
                conn.execute("CREATE TABLE back(user_id TEXT,goods_id INTEGER,goods_name TEXT,goods_type TEXT,goods_num INTEGER,create_time TEXT,update_time TEXT,bind_num INTEGER,PRIMARY KEY(user_id,goods_id))")
                conn.execute("INSERT INTO back VALUES('u',20001,'礼包','礼包',1,NULL,NULL,0)")
            with DatabaseUnitOfWork(database) as uow:
                apply_platform_schema(uow)
                apply_package_reward(uow)
            application = BackApplication(database, database)
            rewards = (PackageReward(1001, "测试奖励", "材料", 1),)
            first = application.open_package(operation_id="back-package-1", user_id="u", package_id=20001, quantity=1, rewards=rewards, max_goods_num=99)
            replay = application.open_package(operation_id="back-package-1", user_id="u", package_id=20001, quantity=1, rewards=rewards, max_goods_num=99)
            self.assertEqual((first.status, replay.status), ("applied", "replayed"))
