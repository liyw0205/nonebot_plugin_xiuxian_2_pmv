import tempfile
import unittest
from pathlib import Path

from ..application import TradeApplication


class Repo:
    def invoke(self, action, *args, **kwargs): return {"status": "duplicate", "action": action}


class TradeApplicationTest(unittest.TestCase):
    def test_duplicate_is_replayable(self):
        with tempfile.TemporaryDirectory() as directory:
            app = TradeApplication(Path(directory) / "game.db", Path(directory) / "trade.db", repository=Repo())
            result = app.deposit(operation_id="trade-1", user_id="u", amount=1)
            self.assertTrue(result.ok)
            replay = app.deposit(operation_id="trade-1", user_id="u", amount=1)
            self.assertTrue(replay.replayed)


if __name__ == "__main__": unittest.main()
