from __future__ import annotations

import tempfile
import unittest

import nonebot

nonebot.init()

from ..application import LunhuiApplication
from ..repository import LunhuiRepository
from ....infrastructure.database import DatabaseUnitOfWork
from ....plugin import apply_platform_schema


class LunhuiApplicationTest(unittest.TestCase):
    def test_repository_defers_legacy_service_import_until_use(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            repository = LunhuiRepository(f"{directory}/game.db")
            self.assertIsNone(repository._reset_service)
            self.assertIsNone(repository._recall_service)
            self.assertIsNone(repository._settle_service)

    def test_recall_result_is_exposed_by_application_boundary(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            database = f"{directory}/game.db"
            with DatabaseUnitOfWork(database) as uow:
                uow.execute(
                    "CREATE TABLE lunhui_recall_operations("
                    "operation_id TEXT PRIMARY KEY,payload TEXT,skill_id INTEGER)"
                )
                uow.execute(
                    "INSERT INTO lunhui_recall_operations VALUES (?, ?, ?)",
                    ("recall-1", "payload", 42),
                )
            result = LunhuiApplication(database).recall_result("recall-1")
            self.assertEqual((result.status, result.skill_id), ("duplicate", 42))

    def test_execute_is_idempotent(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            database = f"{directory}/game.db"
            with DatabaseUnitOfWork(database) as uow:
                apply_platform_schema(uow)
            app = LunhuiApplication(database)
            first = app.execute(operation_id="op-1", user_id="u")
            second = app.execute(operation_id="op-1", user_id="u")
            self.assertEqual(first.operation_id, second.operation_id)
            self.assertTrue(second.replayed)


if __name__ == "__main__":
    unittest.main()
