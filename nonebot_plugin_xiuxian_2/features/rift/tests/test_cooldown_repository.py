from __future__ import annotations

import tempfile
import unittest
from pathlib import Path

from ....infrastructure.database import DatabaseUnitOfWork
from ..cooldown_repository import RiftCooldownSqlRepository
from tests.test_db_backend import db_backend


class RiftCooldownRepositoryTests(unittest.TestCase):
    def test_reads_projection_without_mutating_or_creating_schema(self) -> None:
        with tempfile.TemporaryDirectory(prefix="rift-cooldown-") as directory:
            database = Path(directory) / "game.db"
            repository = RiftCooldownSqlRepository(database)
            self.assertIsNone(repository.read("u"))
            with db_backend.transaction(database) as conn:
                conn.execute(
                    "CREATE TABLE user_cd(user_id TEXT PRIMARY KEY,type INTEGER,"
                    "create_time TEXT,scheduled_time TEXT)"
                )
                conn.execute(
                    "INSERT INTO user_cd VALUES('u',3,'started','60')"
                )
            self.assertEqual(
                repository.read("u"),
                {"type": 3, "create_time": "started", "scheduled_time": "60"},
            )
            self.assertIsNone(repository.read("missing"))

    def test_missing_columns_are_read_only_compatibility_miss(self) -> None:
        with tempfile.TemporaryDirectory(prefix="rift-cooldown-") as directory:
            database = Path(directory) / "game.db"
            with DatabaseUnitOfWork(database) as uow:
                uow.execute("CREATE TABLE user_cd(user_id TEXT PRIMARY KEY)")
            self.assertIsNone(RiftCooldownSqlRepository(database).read("u"))
            with db_backend.connection(database) as conn:
                self.assertEqual(
                    {row[1] for row in conn.execute("PRAGMA table_info(user_cd)")},
                    {"user_id"},
                )


if __name__ == "__main__":
    unittest.main()
