from __future__ import annotations

import tempfile
import unittest
from pathlib import Path

from nonebot_plugin_xiuxian_2.core.result import OperationOutcome
from nonebot_plugin_xiuxian_2.features.auction.settlement import AuctionSettlementApplication
from nonebot_plugin_xiuxian_2.infrastructure.database import DatabaseUnitOfWork


class _Repository:
    def __init__(self, status="settled"):
        self.status = status
        self.calls = 0

    def settle_active(self, operation_id, *, end_time, fee_rate, item_types):
        self.calls += 1
        return {"status": self.status, "results": [{"auction_id": "a-1"}]}


class AuctionSettlementApplicationTests(unittest.TestCase):
    def test_settlement_is_audited_and_idempotent(self):
        with tempfile.TemporaryDirectory() as directory:
            repo = _Repository()
            app = AuctionSettlementApplication(Path(directory) / "game.db", repository=repo)
            first = app.settle_active(operation_id="op-1", end_time=100.0, fee_rate=0.1)
            second = app.settle_active(operation_id="op-1", end_time=100.0, fee_rate=0.1)
            self.assertTrue(first.ok)
            self.assertTrue(second.replayed)
            self.assertEqual(repo.calls, 1)
            with DatabaseUnitOfWork(Path(directory) / "game.db") as uow:
                ledger = uow.query_one("SELECT status FROM operation_ledger WHERE operation_id=?", ("op-1",))
                audit = uow.query_one("SELECT category FROM operation_audit WHERE operation_id=?", ("op-1",))
            self.assertEqual(ledger["status"], "applied")
            self.assertEqual(audit["category"], "auction_settlement")

    def test_business_rejection_is_stable(self):
        with tempfile.TemporaryDirectory() as directory:
            repo = _Repository("inventory_full")
            app = AuctionSettlementApplication(Path(directory) / "game.db", repository=repo)
            result = app.settle_active(operation_id="op-2", end_time=100.0, fee_rate=0.1)
            replay = app.settle_active(operation_id="op-2", end_time=100.0, fee_rate=0.1)
            self.assertFalse(result.ok)
            self.assertEqual(result.code, "inventory_full")
            self.assertEqual(replay.code, "inventory_full")
            self.assertEqual(repo.calls, 1)

    def test_invalid_parameters_are_rejected_before_storage(self):
        with tempfile.TemporaryDirectory() as directory:
            repo = _Repository()
            app = AuctionSettlementApplication(Path(directory) / "game.db", repository=repo)
            with self.assertRaises(Exception):
                app.settle_active(operation_id="", end_time=100.0, fee_rate=0.1)
            self.assertEqual(repo.calls, 0)

    def test_lookup_does_not_create_ledger_schema(self):
        with tempfile.TemporaryDirectory() as directory:
            database = Path(directory) / "empty.db"
            app = AuctionSettlementApplication(database)
            self.assertIsNone(app.lookup("missing"))
            with DatabaseUnitOfWork(database) as uow:
                table = uow.query_one("SELECT name FROM sqlite_master WHERE type='table' AND name='operation_ledger'")
            self.assertIsNone(table)


if __name__ == "__main__":
    unittest.main()
