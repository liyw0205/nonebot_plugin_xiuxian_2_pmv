from __future__ import annotations

import tempfile
import unittest
from pathlib import Path
from types import SimpleNamespace

from nonebot_plugin_xiuxian_2.features.auction.application import AuctionBidApplication
from nonebot_plugin_xiuxian_2.compatibility.auction_bid_effects import LegacyAuctionBidEffects, log_auction_bid_once
from nonebot_plugin_xiuxian_2.core.result import OperationOutcome
from nonebot_plugin_xiuxian_2.infrastructure.database import DatabaseUnitOfWork
from nonebot_plugin_xiuxian_2.infrastructure.database import OperationLedger, ReconcileService
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

    def test_outbox_dispatches_effect_once_after_success(self):
        effects = FakeEffects()
        app = AuctionBidApplication(self.db, repository=FakeRepository(), effects=effects)
        first = self.request(app)
        replay = self.request(app)
        self.assertEqual((first.status, replay.status), ("applied", "replayed"))
        self.assertEqual(len(effects.calls), 1)
        self.assertFalse(effects.calls[0]["replayed"])
        with DatabaseUnitOfWork(self.db) as uow:
            row = uow.query_one("SELECT status FROM domain_outbox WHERE event_type='auction.bid.effects'")
        self.assertEqual(row["status"], "sent")

    def test_legacy_applied_ledger_without_outbox_is_not_projected_again(self):
        effects = FakeEffects()
        app = AuctionBidApplication(self.db, repository=FakeRepository(), effects=effects)
        payload = {
            "auction_id": "a",
            "bidder_id": "u",
            "bid_price": 300,
            "expected_price": 200,
            "expected_bids": {"old": 200},
        }
        with DatabaseUnitOfWork(self.db, immediate=True) as uow:
            ledger = OperationLedger()
            ledger.begin(uow, "op", "auction.bid", payload)
            ledger.finish(
                uow,
                OperationOutcome.applied(
                    "op",
                    "auction.bid",
                    data={"status": "bid", "bid_price": 300, "debit": 100},
                ),
            )

        replay = self.request(app)

        self.assertEqual(replay.status, "replayed")
        self.assertEqual(effects.calls, [])
        with DatabaseUnitOfWork(self.db) as uow:
            event = uow.query_one("SELECT 1 AS present FROM domain_outbox WHERE event_id='op:auction.bid'")
        self.assertIsNone(event)

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
        effects = FakeEffects(error=RuntimeError("projection down"))
        app = AuctionBidApplication(
            self.db,
            repository=FakeRepository(),
            effects=effects,
        )
        result = self.request(app)
        self.assertEqual(result.status, "applied")
        self.assertIn("资产已结算", result.message)
        with DatabaseUnitOfWork(self.db) as uow:
            row = uow.query_one(
                "SELECT status FROM operation_ledger WHERE operation_id='op' AND action='auction.bid'"
            )
            event = uow.query_one("SELECT status,attempts FROM domain_outbox WHERE event_type='auction.bid.effects'")
        self.assertEqual(row["status"], "applied")
        self.assertEqual((event["status"], event["attempts"]), ("pending", 1))
        effects.error = None
        with DatabaseUnitOfWork(self.db) as uow:
            report = ReconcileService().run(
                uow,
                handlers={AuctionBidApplication.effects_event: app.reconcile_outbox_event},
            )
            event = uow.query_one("SELECT status FROM domain_outbox WHERE event_type='auction.bid.effects'")
        self.assertTrue(report.clean)
        self.assertEqual(event["status"], "sent")

    def test_legacy_effect_adapter_replay_is_idempotent_at_log_sink(self):
        players_dir = Path(self.temp.name) / "players"
        effects = LegacyAuctionBidEffects(
            self.db,
            log_writer=lambda **kwargs: log_auction_bid_once(**kwargs, players_dir=players_dir),
        )
        kwargs = {
            "operation_id": "op",
            "auction_id": "a",
            "bidder_id": "u",
            "item_name": "item",
            "bid_price": 300,
            "occurred_at": "2026-09-24T00:00:00+00:00",
        }
        # Avoid requiring the player statistics migration for this adapter
        # delegation test; the repository behavior has its own coverage.
        effects.statistics = type("Stats", (), {"record_bid": lambda *_args, **_kwargs: True})()
        effects.on_bid(**kwargs, replayed=False)
        effects.on_bid(**kwargs, replayed=True)
        log_file = players_dir / "u" / "logs" / "260924.log"
        records = __import__("json").loads(log_file.read_text(encoding="utf-8"))
        self.assertEqual(len(records), 1)
        self.assertEqual(records[0]["event_id"], "auction.bid:op")


if __name__ == "__main__":
    unittest.main()
