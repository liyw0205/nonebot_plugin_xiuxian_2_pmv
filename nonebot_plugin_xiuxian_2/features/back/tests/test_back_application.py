import tempfile
import unittest
from pathlib import Path

from ..application import BackApplication


class Repo:
    def invoke(self, action, *args, **kwargs): return {"status": "completed", "action": action}


class BackApplicationTest(unittest.TestCase):
    def test_completed_maps_to_applied(self):
        with tempfile.TemporaryDirectory() as directory:
            app = BackApplication(Path(directory) / "game.db", Path(directory) / "player.db", repository=Repo())
            self.assertTrue(app.use_item(operation_id="back-1", user_id="u").ok)


if __name__ == "__main__": unittest.main()
