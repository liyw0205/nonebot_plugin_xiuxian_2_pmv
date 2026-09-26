import tempfile
import unittest
from pathlib import Path

from ..application import BackApplication
from ....infrastructure.database import DatabaseUnitOfWork
from ....plugin import apply_platform_schema


class Repo:
    def invoke(self, action, *args, **kwargs): return {"status": "completed", "action": action}


class BackApplicationTest(unittest.TestCase):
    def test_completed_maps_to_applied(self):
        with tempfile.TemporaryDirectory() as directory:
            database = Path(directory) / "game.db"
            with DatabaseUnitOfWork(database) as uow:
                apply_platform_schema(uow)
            app = BackApplication(database, Path(directory) / "player.db", repository=Repo())
            self.assertTrue(app.use_item(operation_id="back-1", user_id="u").ok)


if __name__ == "__main__": unittest.main()
