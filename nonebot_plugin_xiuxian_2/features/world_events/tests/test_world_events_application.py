from __future__ import annotations

import tempfile
import unittest
from pathlib import Path

from ..application import DemonClaimApplication
from ....infrastructure.database import DatabaseUnitOfWork, OperationLedger


class _Repository:
    def __init__(self, status="applied"):
        self.status = status
        self.calls = 0

    def claim(self, operation_id, event_key, event_id, user_id, expected_claimed, stone, exp, items, max_goods_num):
        self.calls += 1
        return {"status": self.status, "stone": stone, "exp": exp}


class DemonClaimApplicationTests(unittest.TestCase):
    def test_claim_is_idempotent(self):
        with tempfile.TemporaryDirectory() as directory:
            repository = _Repository()
            game = Path(directory) / "game.db"
            with DatabaseUnitOfWork(game) as uow:
                OperationLedger().ensure_schema(uow)
            app = DemonClaimApplication(game, Path(directory) / "player.db", repository=repository)
            kwargs = {
                "operation_id": "demon-1", "event_key": "global", "event_id": "demon:2026-09-12",
                "user_id": "u", "expected_claimed": {}, "stone": 100, "exp": 200,
                "items": ({"id": 1, "name": "符", "type": "特殊物品", "amount": 1},), "max_goods_num": 100,
            }
            first = app.claim(**kwargs)
            second = app.claim(**kwargs)
            self.assertTrue(first.ok)
            self.assertTrue(second.replayed)
            self.assertEqual(first.granted["stone"], 100)
            self.assertEqual(repository.calls, 1)

    def test_inventory_rejection_replays_as_rejection(self):
        with tempfile.TemporaryDirectory() as directory:
            repository = _Repository("inventory_full")
            game = Path(directory) / "game.db"
            with DatabaseUnitOfWork(game) as uow:
                OperationLedger().ensure_schema(uow)
            app = DemonClaimApplication(game, Path(directory) / "player.db", repository=repository)
            kwargs = {
                "operation_id": "demon-2", "event_key": "global", "event_id": "demon:2026-09-12",
                "user_id": "u", "expected_claimed": {}, "stone": 0, "exp": 0, "items": (), "max_goods_num": 100,
            }
            first = app.claim(**kwargs)
            second = app.claim(**kwargs)
            self.assertFalse(first.ok)
            self.assertEqual(second.code, "inventory_full")
            self.assertEqual(repository.calls, 1)


if __name__ == "__main__":
    unittest.main()
