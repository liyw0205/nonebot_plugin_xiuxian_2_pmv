from __future__ import annotations

import tempfile
import unittest
from pathlib import Path

from ..application import DungeonApplication


class _Repository:
    def __init__(self, status: str = "applied") -> None:
        self.status = status
        self.calls: list[str] = []

    def purchase(self, *args, **kwargs):
        self.calls.append("purchase")
        return {"status": self.status, "quantity": 1, "cost": 10, "stone": 90, "inventory": 1, "response": "ok"}

    def operation_result(self, *args, **kwargs):
        return None

    def replay(self, *args, **kwargs):
        self.calls.append("replay")
        return type("Result", (), {"status": "missing", "phase": ""})()

    def prepare(self, *args, **kwargs):
        self.calls.append("prepare")
        return type("Result", (), {"status": "prepared", "phase": "prepared"})()

    def settle(self, *args, **kwargs):
        self.calls.append("settle")
        return type("Result", (), {"status": "applied", "phase": "completed"})()

    def resolve_rejection(self, *args, **kwargs):
        self.calls.append("reject")
        return type("Result", (), {"status": "applied", "phase": "completed"})()


class DungeonApplicationTests(unittest.TestCase):
    def test_purchase_is_idempotent(self):
        with tempfile.TemporaryDirectory() as directory:
            repository = _Repository()
            app = DungeonApplication(Path(directory) / "game.db", Path(directory) / "player.db", repository=repository)
            request = dict(operation_id="dungeon-buy-1", user_id="u", item_id=1999, item_name="渡厄丹", item_type="丹药", quantity=1, unit_cost=10, expected_stone=100, max_goods=99)
            first = app.purchase(**request)
            replay = app.purchase(**request)
            self.assertTrue(first.ok and replay.replayed)
            self.assertEqual(repository.calls, ["purchase"])

    def test_explore_boundary_delegates_with_operation_id(self):
        with tempfile.TemporaryDirectory() as directory:
            repository = _Repository()
            app = DungeonApplication(Path(directory) / "game.db", Path(directory) / "player.db", repository=repository)
            self.assertEqual(app.replay(operation_id="explore-1", user_id="u").status, "missing")
            self.assertEqual(app.prepare(operation_id="explore-1", user_id="u", plan={"expected_status": {"current_layer": 0}}).phase, "prepared")
            self.assertEqual(app.settle(operation_id="explore-1", user_id="u", max_goods_num=99).phase, "completed")
            self.assertEqual(repository.calls, ["replay", "prepare", "settle"])


if __name__ == "__main__":
    unittest.main()
