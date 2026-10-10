from __future__ import annotations

import tempfile
import unittest
from pathlib import Path

from ....infrastructure.database import DatabaseUnitOfWork
from ....plugin import apply_platform_schema
from ..application import AuctionBidApplication
from ..bid_repository import AuctionBidSqlRepository
from ..settlement import AuctionSettlementApplication


class AuctionFeatureContractTests(unittest.TestCase):
    def test_application_defers_legacy_repository_import(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            database = Path(directory) / "game.db"
            application = AuctionBidApplication(database)
            self.assertIsInstance(application.repository, AuctionBidSqlRepository)
            self.assertFalse(database.exists())

    def test_settlement_uses_injected_repository(self) -> None:
        class Repository:
            def settle_active(self, operation_id, *, end_time, fee_rate, item_types):
                return {"status": "empty", "results": []}

        with tempfile.TemporaryDirectory() as directory:
            database = Path(directory) / "game.db"
            with DatabaseUnitOfWork(database) as uow:
                apply_platform_schema(uow)
            result = AuctionSettlementApplication(
                database, repository=Repository()
            ).settle_active(operation_id="settle-1", end_time=1, fee_rate=0.1)
            self.assertTrue(result.ok)


if __name__ == "__main__":
    unittest.main()
