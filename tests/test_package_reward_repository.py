import tempfile
import unittest
from pathlib import Path

from nonebot_plugin_xiuxian_2.features.package_reward.migrations import apply_package_reward
from nonebot_plugin_xiuxian_2.features.package_reward.repository import PackageRewardRepository
from nonebot_plugin_xiuxian_2.infrastructure.database import DatabaseUnitOfWork


class PackageRewardRepositoryTests(unittest.TestCase):
    def test_operation_requires_startup_schema(self):
        with tempfile.TemporaryDirectory() as directory:
            database = Path(directory) / "game.db"
            repository = PackageRewardRepository()
            with DatabaseUnitOfWork(database) as uow:
                apply_package_reward(uow)
                self.assertIsNone(repository.operation(uow, "missing"))

    def test_operation_does_not_create_schema_at_request_time(self):
        with tempfile.TemporaryDirectory() as directory:
            database = Path(directory) / "empty.db"
            repository = PackageRewardRepository()
            with DatabaseUnitOfWork(database) as uow:
                with self.assertRaises(Exception):
                    repository.operation(uow, "missing")
            with DatabaseUnitOfWork(database) as uow:
                table = uow.query_one("SELECT name FROM sqlite_master WHERE type='table' AND name='package_reward_operations'")
            self.assertIsNone(table)


if __name__ == "__main__":
    unittest.main()
