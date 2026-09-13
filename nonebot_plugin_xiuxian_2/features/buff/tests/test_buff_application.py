import tempfile
import unittest
from pathlib import Path

from ..application import BuffApplication


class Repo:
    def invoke(self, action, *args, **kwargs): return {"status": "applied", "action": action}


class BuffApplicationTest(unittest.TestCase):
    def test_operation_is_idempotent(self):
        with tempfile.TemporaryDirectory() as directory:
            app = BuffApplication(Path(directory) / "game.db", Path(directory) / "player.db", repository=Repo())
            self.assertTrue(app.open(operation_id="buff-1", user_id="u").ok)
            self.assertTrue(app.open(operation_id="buff-1", user_id="u").replayed)


if __name__ == "__main__": unittest.main()
