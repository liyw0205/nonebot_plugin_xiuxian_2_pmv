import tempfile
import unittest
from pathlib import Path

from ..application import BaseApplication
from ....infrastructure.database import DatabaseUnitOfWork, OperationLedger


class Repo:
    def invoke(self, action, *args, **kwargs): return {"status": "rejected", "message": "state changed"}


class BaseApplicationTest(unittest.TestCase):
    def test_rejection_is_non_success(self):
        with tempfile.TemporaryDirectory() as directory:
            database = Path(directory) / "game.db"
            with DatabaseUnitOfWork(database, immediate=True) as uow:
                OperationLedger().ensure_schema(uow)
            app = BaseApplication(database, Path(directory) / "player.db", repository=Repo())
            result = app.breakthrough(operation_id="base-1", user_id="u")
            self.assertFalse(result.ok)
            self.assertEqual(result.code, "rejected")


if __name__ == "__main__": unittest.main()
