import tempfile
import unittest
from pathlib import Path

from ..application import BaseApplication


class Repo:
    def invoke(self, action, *args, **kwargs): return {"status": "rejected", "message": "state changed"}


class BaseApplicationTest(unittest.TestCase):
    def test_rejection_is_non_success(self):
        with tempfile.TemporaryDirectory() as directory:
            app = BaseApplication(Path(directory) / "game.db", Path(directory) / "player.db", repository=Repo())
            result = app.breakthrough(operation_id="base-1", user_id="u")
            self.assertFalse(result.ok)
            self.assertEqual(result.code, "rejected")


if __name__ == "__main__": unittest.main()
