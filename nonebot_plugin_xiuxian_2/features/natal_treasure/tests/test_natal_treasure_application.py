import tempfile
import unittest
from pathlib import Path

from ..application import NatalTreasureApplication


class Repo:
    def awaken(self, *args, **kwargs): return {"status": "applied", "granted": {"slot": 1}}


class NatalTreasureApplicationTest(unittest.TestCase):
    def test_success_and_replay(self):
        with tempfile.TemporaryDirectory() as directory:
            app = NatalTreasureApplication(Path(directory) / "player.db", Path(directory) / "game.db", repository=Repo())
            first = app.awaken(operation_id="natal-1", user_id="u")
            second = app.awaken(operation_id="natal-1", user_id="u")
            self.assertTrue(first.ok)
            self.assertTrue(second.replayed)


if __name__ == "__main__": unittest.main()
