from __future__ import annotations

import tempfile
import unittest
from pathlib import Path

from ..application import TowerApplication


class _Repository:
    def __init__(self):
        self.calls = []

    def purchase(self, *args, **kwargs):
        self.calls.append("purchase")
        return {"status": "applied", "quantity": 2, "cost": 20, "score": 80, "purchased": 2, "inventory": 2}

    def settle(self, *args, **kwargs):
        self.calls.append("settle")
        return {"status": "applied", "score": 8, "stone": 20, "exp": 30, "floor": 10, "challenge_succeeded": True, "rewards": []}


class TowerApplicationTests(unittest.TestCase):
    def test_purchase_and_settlement_replay(self):
        with tempfile.TemporaryDirectory() as directory:
            repository = _Repository()
            app = TowerApplication(Path(directory) / "game.db", Path(directory) / "player.db", repository=repository)
            purchase = dict(operation_id="purchase-1", user_id="u", item_id=1, item_name="灵草", item_type="药材", quantity=2, unit_cost=10, weekly_limit=5, expected_score=100, expected_weekly_purchases={"_last_reset": "2026-09-12"}, max_goods_num=99)
            first = app.purchase(**purchase)
            replay = app.purchase(**purchase)
            settle = dict(operation_id="settle-1", user_id="u", expected_tower={"current_floor": 9, "max_floor": 9, "score": 50}, floor=10, score=8, stone=20, exp=30, items=[], max_goods_num=99)
            settled = app.settle(**settle)
            self.assertTrue(first.ok and replay.replayed and settled.ok)
            self.assertEqual(repository.calls, ["purchase", "settle"])


if __name__ == "__main__":
    unittest.main()
