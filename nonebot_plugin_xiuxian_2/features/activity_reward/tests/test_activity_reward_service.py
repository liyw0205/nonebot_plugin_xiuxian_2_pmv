from __future__ import annotations

import tempfile
import unittest
from pathlib import Path

from ..application import ActivityRewardApplication
from ....infrastructure.database import DatabaseUnitOfWork
from ....plugin import apply_platform_schema


class _FakeRepository:
    def claim_all(self, operation_id: str, user_id: str):
        return True, "ok"


class ActivityRewardServiceTests(unittest.TestCase):
    def test_application_returns_structured_result(self):
        with tempfile.TemporaryDirectory() as directory:
            database = Path(directory) / "game.db"
            with DatabaseUnitOfWork(database) as uow:
                apply_platform_schema(uow)
            result = ActivityRewardApplication(
                database, repository=_FakeRepository()
            ).claim_all(operation_id="activity-feature-1", user_id="u")
        self.assertTrue(result.ok)
        self.assertEqual(result.data["user_id"], "u")


if __name__ == "__main__":
    unittest.main()
