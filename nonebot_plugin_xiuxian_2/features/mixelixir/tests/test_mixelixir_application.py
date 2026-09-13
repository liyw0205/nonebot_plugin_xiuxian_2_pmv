from __future__ import annotations

import tempfile
import unittest
from pathlib import Path

from ..application import MixelixirApplication


class _Repository:
    def __init__(self):
        self.calls = []

    def harvest(self, *args, **kwargs):
        self.calls.append("harvest")
        return {"status": "applied", "harvested_at": "new", "rewards": [{"item_id": 1, "name": "草", "quantity": 2}]}

    def settle(self, *args, **kwargs):
        self.calls.append("settle")
        return {"status": "applied", "reward_quantity": 3}


class MixelixirApplicationTests(unittest.TestCase):
    def test_harvest_and_settlement_are_idempotent(self):
        with tempfile.TemporaryDirectory() as directory:
            repository = _Repository()
            app = MixelixirApplication(Path(directory) / "game.db", Path(directory) / "player.db", repository=repository)
            harvest_kwargs = {"operation_id": "harvest-1", "user_id": "u", "expected_last_time": "old", "harvested_at": "new", "rewards": ((1, "草", 2),), "max_goods_num": 99}
            first = app.harvest(**harvest_kwargs)
            replay = app.harvest(**harvest_kwargs)
            settled = app.settle(operation_id="settle-1", user_id="u", materials={1: 2}, reward_id=2, reward_name="丹", reward_quantity=3, max_goods_num=99)
            self.assertTrue(first.ok and replay.replayed and settled.ok)
            self.assertEqual(repository.calls, ["harvest", "settle"])


if __name__ == "__main__":
    unittest.main()
