import tempfile
import unittest
from pathlib import Path

from ..application import TradeApplication
from ....infrastructure.database import DatabaseUnitOfWork
from ....plugin import apply_platform_schema


class Repo:
    def invoke(self, action, *args, **kwargs): return {"status": "duplicate", "action": action}


class TradeApplicationTest(unittest.TestCase):
    def test_duplicate_is_replayable(self):
        with tempfile.TemporaryDirectory() as directory:
            database = Path(directory) / "game.db"
            with DatabaseUnitOfWork(database) as uow:
                apply_platform_schema(uow)
            app = TradeApplication(database, Path(directory) / "trade.db", repository=Repo())
            result = app.deposit(operation_id="trade-1", user_id="u", amount=1)
            self.assertTrue(result.ok)
            replay = app.deposit(operation_id="trade-1", user_id="u", amount=1)
            self.assertTrue(replay.replayed)


if __name__ == "__main__": unittest.main()
