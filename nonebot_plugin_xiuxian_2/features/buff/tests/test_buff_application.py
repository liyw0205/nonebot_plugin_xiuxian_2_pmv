import tempfile
import unittest
from pathlib import Path

from ..application import BuffApplication
from ....infrastructure.database import DatabaseUnitOfWork, OperationLedger


class Repo:
    def invoke(self, action, *args, **kwargs): return {"status": "applied", "action": action}


class BuffApplicationTest(unittest.TestCase):
    def test_operation_is_idempotent(self):
        with tempfile.TemporaryDirectory() as directory:
            database = Path(directory) / "game.db"
            with DatabaseUnitOfWork(database, immediate=True) as uow:
                OperationLedger().ensure_schema(uow)
            app = BuffApplication(database, Path(directory) / "player.db", repository=Repo())
            self.assertTrue(app.open(operation_id="buff-1", user_id="u").ok)
            self.assertTrue(app.open(operation_id="buff-1", user_id="u").replayed)


if __name__ == "__main__": unittest.main()
