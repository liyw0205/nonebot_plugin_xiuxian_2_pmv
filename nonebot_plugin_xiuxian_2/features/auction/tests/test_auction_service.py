from __future__ import annotations

import tempfile
import unittest
from pathlib import Path

from ..application import AuctionBidApplication
from ..settlement import AuctionSettlementApplication


class AuctionFeatureContractTests(unittest.TestCase):
    def test_application_defers_legacy_repository_import(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            application = AuctionBidApplication(Path(directory) / "game.db")
            self.assertIsNone(application.repository)

    def test_settlement_uses_injected_repository(self) -> None:
        class Repository:
            def settle_active(self, operation_id, *, end_time, fee_rate, item_types):
                return {"status": "empty", "results": []}

        with tempfile.TemporaryDirectory() as directory:
            result = AuctionSettlementApplication(
                Path(directory) / "game.db", repository=Repository()
            ).settle_active(operation_id="settle-1", end_time=1, fee_rate=0.1)
            self.assertTrue(result.ok)


if __name__ == "__main__":
    unittest.main()
