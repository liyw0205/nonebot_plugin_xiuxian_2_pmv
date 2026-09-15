import tempfile
import unittest
from pathlib import Path

from nonebot_plugin_xiuxian_2.features.beg.repository import BegRepository
from nonebot_plugin_xiuxian_2.infrastructure.database import DatabaseUnitOfWork


class BegRepositorySchemaTests(unittest.TestCase):
    def test_replay_requires_startup_schema(self):
        with tempfile.TemporaryDirectory() as directory:
            database = Path(directory) / "empty.db"
            repository = BegRepository()
            with DatabaseUnitOfWork(database) as uow:
                with self.assertRaises(Exception):
                    repository.daily_result(uow, "missing")
            with DatabaseUnitOfWork(database) as uow:
                table = uow.query_one("SELECT name FROM sqlite_master WHERE type='table' AND name='beg_daily_reward_operations'")
            self.assertIsNone(table)


if __name__ == "__main__":
    unittest.main()
