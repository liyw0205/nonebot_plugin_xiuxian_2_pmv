import tempfile
import unittest
from pathlib import Path

from ....infrastructure.database import DatabaseUnitOfWork
from ....plugin import apply_platform_schema
from ..application import NatalTreasureApplication


class Repo:
    def awaken(self, *args, **kwargs): return {"status": "applied", "granted": {"slot": 1}}


class NatalTreasureApplicationTest(unittest.TestCase):
    def test_success_and_replay(self):
        with tempfile.TemporaryDirectory() as directory:
            database = Path(directory) / "game.db"
            with DatabaseUnitOfWork(database) as uow:
                apply_platform_schema(uow)
            app = NatalTreasureApplication(Path(directory) / "player.db", database, repository=Repo())
            first = app.awaken(operation_id="natal-1", user_id="u")
            second = app.awaken(operation_id="natal-1", user_id="u")
            self.assertTrue(first.ok)
            self.assertTrue(second.replayed)


if __name__ == "__main__": unittest.main()
