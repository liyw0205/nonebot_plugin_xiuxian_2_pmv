from __future__ import annotations

import tempfile
import unittest
from pathlib import Path
from types import SimpleNamespace

from nonebot_plugin_xiuxian_2.features.auction.application import AuctionBidApplication
from nonebot_plugin_xiuxian_2.features.auction.bid_effects import LegacyAuctionBidEffects
from nonebot_plugin_xiuxian_2.infrastructure.database import DatabaseUnitOfWork
from nonebot_plugin_xiuxian_2.plugin import apply_platform_schema


class FakeRepository:
    def __init__(self, result=None, error=None):
        self.result = result or SimpleNamespace(status="bid", bid_price=300, debit=100, refunded_bidder="old", refunded_amount=200)
        self.error = error

    def place_auction_bid(self, *args):
        if self.error:
            raise self.error
        return self.result


class FakeEffects:
    def __init__(self, error=None):
        self.calls = []
        self.error = error

    def on_bid(self, **kwargs):
        if self.error:
            raise self.error
        self.calls.append(kwargs)


class AuctionApplicationTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.db = Path(self.temp.name) / "game.db"
        with DatabaseUnitOfWork(self.db) as uow:
            apply_platform_schema(uow)

    def tearDown(self):
        self.temp.cleanup()

    def request(self, app, operation_id="op"):
        return app.place_bid(operation_id=operation_id, auction_id="a", bidder_id="u", bid_price=300, expected_price=200, expected_bids={"old": 200}, bid_time=1)

    def test_ledger_result_is_replayed(self):
        app = AuctionBidApplication(self.db, repository=FakeRepository())
        first = self.request(app)
        replay = self.request(app)
        self.assertEqual((first.status, replay.status), ("applied", "replayed"))

    def test_rejected_repository_result_is_audited(self):
        app = AuctionBidApplication(self.db, repository=FakeRepository(SimpleNamespace(status="stone_insufficient")))
        result = self.request(app)
        self.assertEqual((result.status, result.code), ("rejected", "stone_insufficient"))

    def test_exception_is_recorded_as_failed(self):
        app = AuctionBidApplication(self.db, repository=FakeRepository(error=RuntimeError("database down")))
        with self.assertRaises(RuntimeError):
            self.request(app)
        with DatabaseUnitOfWork(self.db) as uow:
            row = uow.query_one("SELECT status FROM operation_ledger WHERE operation_id='op' AND action='auction.bid'")
        self.assertEqual(row["status"], "failed")

    def test_effects_receive_replay_flag(self):
        effects = FakeEffects()
        app = AuctionBidApplication(self.db, repository=FakeRepository(), effects=effects)
        first = self.request(app)
        replay = self.request(app)
        self.assertEqual((first.status, replay.status), ("applied", "replayed"))
        self.assertEqual(len(effects.calls), 2)
        self.assertFalse(effects.calls[0]["replayed"])
        self.assertTrue(effects.calls[1]["replayed"])

    def test_started_operation_replays_repository_to_repair_ledger(self):
        from nonebot_plugin_xiuxian_2.infrastructure.database import OperationLedger

        kwargs = {
            "operation_id": "op",
            "auction_id": "a",
            "bidder_id": "u",
            "bid_price": 300,
            "expected_price": 200,
            "expected_bids": {"old": 200},
            "bid_time": 1,
        }
        with DatabaseUnitOfWork(self.db, immediate=True) as uow:
            OperationLedger().begin(
                uow,
                "op",
                "auction.bid",
                {
                    "auction_id": "a",
                    "bidder_id": "u",
                    "bid_price": 300,
                    "expected_price": 200,
                    "expected_bids": {"old": 200},
                },
            )
        app = AuctionBidApplication(
            self.db,
            repository=FakeRepository(
                SimpleNamespace(status="duplicate", bid_price=300, debit=100)
            ),
        )
        result = app.place_bid(**kwargs)
        self.assertEqual((result.status, result.replayed), ("applied", True))
        with DatabaseUnitOfWork(self.db) as uow:
            row = uow.query_one(
                "SELECT status FROM operation_ledger WHERE operation_id='op' AND action='auction.bid'"
            )
        self.assertEqual(row["status"], "applied")

    def test_effect_failure_does_not_turn_committed_bid_into_retry(self):
        app = AuctionBidApplication(
            self.db,
            repository=FakeRepository(),
            effects=FakeEffects(error=RuntimeError("projection down")),
        )
        result = self.request(app)
        self.assertEqual(result.status, "applied")
        self.assertIn("资产已结算", result.message)
        with DatabaseUnitOfWork(self.db) as uow:
            row = uow.query_one(
                "SELECT status FROM operation_ledger WHERE operation_id='op' AND action='auction.bid'"
            )
        self.assertEqual(row["status"], "applied")

    def test_legacy_effect_adapter_skips_replayed_statistics(self):
        calls = []
        effects = LegacyAuctionBidEffects(lambda *args: calls.append(args))
        kwargs = {
            "operation_id": "op",
            "auction_id": "a",
            "bidder_id": "u",
            "item_name": "item",
            "bid_price": 300,
        }
        effects.on_bid(**kwargs, replayed=False)
        effects.on_bid(**kwargs, replayed=True)
        self.assertEqual(len(calls), 1)
        self.assertEqual(calls[0][0], "u")


if __name__ == "__main__":
    unittest.main()
