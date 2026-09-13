from __future__ import annotations

import tempfile
import unittest
from pathlib import Path

from ..application import PuppetApplication


class _Repository:
    def __init__(self, purchase_status: str = "purchased") -> None:
        self.purchase_status = purchase_status
        self.calls: list[str] = []

    def purchase(self, operation_id, user_id, stone_cost):
        self.calls.append("purchase")
        return {
            "status": self.purchase_status,
            "user_id": user_id,
            "action": "purchase",
            "previous_level": 0,
            "current_level": 1,
            "stone_cost": stone_cost,
        }

    def upgrade(self, operation_id, user_id, upgrade_costs, *, max_level):
        self.calls.append("upgrade")
        return {
            "status": "upgraded",
            "user_id": user_id,
            "action": "upgrade",
            "previous_level": 1,
            "current_level": 2,
            "stone_cost": upgrade_costs[1],
        }


class PuppetApplicationTests(unittest.TestCase):
    def test_purchase_is_idempotent_and_replayed(self):
        with tempfile.TemporaryDirectory() as directory:
            repository = _Repository()
            app = PuppetApplication(Path(directory) / "game.db", Path(directory) / "player.db", repository=repository)
            request = {"operation_id": "puppet-buy-1", "user_id": "u", "stone_cost": 50}
            first = app.purchase(**request)
            replay = app.purchase(**request)
            self.assertTrue(first.ok)
            self.assertTrue(replay.replayed)
            self.assertEqual(repository.calls, ["purchase"])
            self.assertEqual(replay.data["current_level"], 1)

    def test_rejected_operation_is_replayed_without_retrying_repository(self):
        with tempfile.TemporaryDirectory() as directory:
            repository = _Repository("stone_insufficient")
            app = PuppetApplication(Path(directory) / "game.db", Path(directory) / "player.db", repository=repository)
            request = {"operation_id": "puppet-buy-poor", "user_id": "u", "stone_cost": 50}
            first = app.purchase(**request)
            replay = app.purchase(**request)
            self.assertFalse(first.ok)
            self.assertEqual(first.code, "stone_insufficient")
            self.assertTrue(replay.replayed)
            self.assertEqual(repository.calls, ["purchase"])

    def test_upgrade_uses_application_contract(self):
        with tempfile.TemporaryDirectory() as directory:
            repository = _Repository()
            app = PuppetApplication(Path(directory) / "game.db", Path(directory) / "player.db", repository=repository)
            result = app.upgrade(operation_id="puppet-upgrade-1", user_id="u", upgrade_costs={1: 60, 2: 90}, max_level=3)
            self.assertTrue(result.ok)
            self.assertEqual(result.data["current_level"], 2)
            self.assertEqual(result.consumed["stone"], 60)
            self.assertEqual(repository.calls, ["upgrade"])

    def test_invalid_request_is_rejected_before_repository(self):
        with tempfile.TemporaryDirectory() as directory:
            repository = _Repository()
            app = PuppetApplication(Path(directory) / "game.db", Path(directory) / "player.db", repository=repository)
            with self.assertRaises(Exception):
                app.purchase(operation_id="", user_id="u", stone_cost=1)
            self.assertEqual(repository.calls, [])


if __name__ == "__main__":
    unittest.main()
