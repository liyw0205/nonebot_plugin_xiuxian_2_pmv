from __future__ import annotations

import tempfile
import unittest
from pathlib import Path

from nonebot_plugin_xiuxian_2.core.result import OperationOutcome
from nonebot_plugin_xiuxian_2.features.auction.settlement import AuctionSettlementApplication
from nonebot_plugin_xiuxian_2.infrastructure.database import DatabaseUnitOfWork, OutboxStore, ReconcileService
from nonebot_plugin_xiuxian_2.plugin import apply_platform_schema


class _Repository:
    def __init__(self, status="settled", results=None):
        self.status = status
        self.results = results or [{"auction_id": "a-1"}]
        self.calls = 0

    def settle_active(self, operation_id, *, end_time, fee_rate, item_types):
        self.calls += 1
        return {"status": self.status, "results": self.results}


class _Effects:
    def __init__(self, *, fail_once=False):
        self.fail_once = fail_once
        self.calls = []
        self.attempts = 0

    def on_settlement(self, **event):
        self.attempts += 1
        if self.fail_once:
            self.fail_once = False
            raise RuntimeError("temporary projection failure")
        self.calls.append(event)


class AuctionSettlementApplicationTests(unittest.TestCase):
    def test_settlement_is_audited_and_idempotent(self):
        with tempfile.TemporaryDirectory() as directory:
            repo = _Repository()
            database = Path(directory) / "game.db"
            with DatabaseUnitOfWork(database) as uow:
                apply_platform_schema(uow)
            app = AuctionSettlementApplication(database, repository=repo)
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
            database = Path(directory) / "game.db"
            with DatabaseUnitOfWork(database) as uow:
                apply_platform_schema(uow)
            app = AuctionSettlementApplication(database, repository=repo)
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

    def test_settlement_events_are_per_role_and_replay_does_not_repeat_effects(self):
        results = [
            {
                "auction_id": "a-1", "item_id": 1, "item_name": "灵剑",
                "final_price": 300, "winner_id": "buyer", "seller_id": "seller",
            },
            {
                "auction_id": "a-2", "item_id": 2, "item_name": "灵草",
                "final_price": None, "winner_id": None, "seller_id": "seller-2",
                "status": "流拍",
            },
            {
                "auction_id": "a-3", "item_id": 3, "item_name": "系统剑",
                "final_price": 500, "winner_id": "buyer-2", "seller_id": "0",
            },
        ]
        with tempfile.TemporaryDirectory() as directory:
            database = Path(directory) / "game.db"
            with DatabaseUnitOfWork(database) as uow:
                apply_platform_schema(uow)
            effects = _Effects()
            app = AuctionSettlementApplication(
                database, repository=_Repository(results=results), effects=effects
            )

            first = app.settle_active(operation_id="settle-1", end_time=100.0, fee_rate=0.1)
            replay = app.settle_active(operation_id="settle-1", end_time=100.0, fee_rate=0.1)

            self.assertTrue(first.ok)
            self.assertTrue(replay.replayed)
            self.assertEqual(
                [event["event_key"] for event in effects.calls],
                ["winner", "seller", "seller_miss", "winner"],
            )
            with DatabaseUnitOfWork(database) as uow:
                events = uow.query_all(
                    "SELECT event_id,status FROM domain_outbox "
                    "WHERE event_type=? ORDER BY event_id",
                    (AuctionSettlementApplication.effects_event,),
                )
            self.assertEqual(len(events), 4)
            self.assertTrue(all(row["status"] == "sent" for row in events))

    def test_started_settlement_recovers_repository_duplicate_and_emits_outbox(self):
        result = {
            "auction_id": "a-1", "item_id": 1, "item_name": "灵剑",
            "final_price": 300, "winner_id": "buyer", "seller_id": "seller",
        }
        with tempfile.TemporaryDirectory() as directory:
            database = Path(directory) / "game.db"
            with DatabaseUnitOfWork(database) as uow:
                apply_platform_schema(uow)
            repo = _Repository("duplicate", [result])
            effects = _Effects()
            app = AuctionSettlementApplication(database, repository=repo, effects=effects)
            with DatabaseUnitOfWork(database, immediate=True) as uow:
                app.ledger.begin(
                    uow,
                    "settle-recover",
                    app.action,
                    {"end_time": 100.0, "fee_rate": 0.1, "item_types": {}},
                )

            outcome = app.settle_active(
                operation_id="settle-recover", end_time=100.0, fee_rate=0.1
            )

            self.assertTrue(outcome.ok)
            self.assertTrue(outcome.replayed)
            self.assertEqual(repo.calls, 1)
            self.assertEqual([event["event_key"] for event in effects.calls], ["winner", "seller"])

    def test_historical_duplicate_without_started_ledger_does_not_replay_effects(self):
        result = {
            "auction_id": "a-1", "item_id": 1, "item_name": "灵剑",
            "final_price": 300, "winner_id": "buyer", "seller_id": "seller",
        }
        with tempfile.TemporaryDirectory() as directory:
            database = Path(directory) / "game.db"
            with DatabaseUnitOfWork(database) as uow:
                apply_platform_schema(uow)
            effects = _Effects()
            app = AuctionSettlementApplication(
                database, repository=_Repository("duplicate", [result]), effects=effects
            )

            app.settle_active(operation_id="legacy-settlement", end_time=100.0, fee_rate=0.1)

            self.assertEqual(effects.calls, [])
            with DatabaseUnitOfWork(database) as uow:
                count = uow.query_one(
                    "SELECT COUNT(*) AS n FROM domain_outbox WHERE event_type=?",
                    (AuctionSettlementApplication.effects_event,),
                )["n"]
            self.assertEqual(count, 0)

    def test_failed_effect_is_reconciled_without_repeating_completed_event(self):
        result = {
            "auction_id": "a-1", "item_id": 1, "item_name": "灵剑",
            "final_price": 300, "winner_id": "buyer", "seller_id": "0",
        }
        with tempfile.TemporaryDirectory() as directory:
            database = Path(directory) / "game.db"
            with DatabaseUnitOfWork(database) as uow:
                apply_platform_schema(uow)
            effects = _Effects(fail_once=True)
            app = AuctionSettlementApplication(
                database, repository=_Repository(results=[result]), effects=effects
            )

            first = app.settle_active(operation_id="settle-reconcile", end_time=100.0, fee_rate=0.1)
            with DatabaseUnitOfWork(database) as uow:
                pending = uow.query_one(
                    "SELECT status,attempts FROM domain_outbox WHERE event_type=?",
                    (app.effects_event,),
                )
            self.assertTrue(first.ok)
            self.assertIn("稍后补偿", first.message)
            self.assertEqual((pending["status"], pending["attempts"]), ("pending", 1))

            with DatabaseUnitOfWork(database, immediate=True) as uow:
                report = ReconcileService().run(
                    uow,
                    handlers={app.effects_event: app.reconcile_outbox_event},
                )

            self.assertTrue(report.clean)
            self.assertEqual(effects.attempts, 2)
            self.assertEqual(len(effects.calls), 1)
            with DatabaseUnitOfWork(database) as uow:
                row = uow.query_one(
                    "SELECT status FROM domain_outbox WHERE event_type=?",
                    (app.effects_event,),
                )
            self.assertEqual(row["status"], "sent")


if __name__ == "__main__":
    unittest.main()
