import tempfile
import unittest
from pathlib import Path

from ..application import RiftApplication


class Repo:
    def invoke(self, action, *args, **kwargs): return {"status": "applied", "action": action}


class RiftApplicationTest(unittest.TestCase):
    def test_enter_is_idempotent(self):
        with tempfile.TemporaryDirectory() as directory:
            app = RiftApplication(Path(directory) / "game.db", Path(directory) / "player.db", repository=Repo())
            self.assertTrue(app.enter(operation_id="rift-1", user_id="u").ok)
            self.assertTrue(app.enter(operation_id="rift-1", user_id="u").replayed)


if __name__ == "__main__": unittest.main()
