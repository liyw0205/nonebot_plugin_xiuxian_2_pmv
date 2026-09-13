import tempfile
import unittest
from pathlib import Path

from ..application import MapApplication


class Repo:
    def invoke(self, action, *args, **kwargs): return {"status": "applied", "action": action}


class MapApplicationTest(unittest.TestCase):
    def test_move_uses_operation_ledger(self):
        with tempfile.TemporaryDirectory() as directory:
            app = MapApplication(Path(directory) / "game.db", Path(directory) / "player.db", repository=Repo())
            self.assertTrue(app.move(operation_id="map-1", user_id="u").ok)


if __name__ == "__main__": unittest.main()
