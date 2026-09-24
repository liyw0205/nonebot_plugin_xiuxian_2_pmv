from __future__ import annotations

import tempfile
import unittest
from contextlib import redirect_stdout
from io import StringIO
from pathlib import Path

from nonebot_plugin_xiuxian_2.adapters.web.app import create_app
from nonebot_plugin_xiuxian_2.bootstrap import build_runtime_context
from nonebot_plugin_xiuxian_2.compatibility.auction_bid_effects import LegacyAuctionBidEffects
from nonebot_plugin_xiuxian_2.features.auction.application import AuctionBidApplication
from nonebot_plugin_xiuxian_2.features.auction.migrations import (
    apply_auction_bid_operations,
    apply_auction_bid_statistics,
    apply_auction_settlement,
)
from nonebot_plugin_xiuxian_2.infrastructure.database import DatabaseUnitOfWork, OutboxStore
from nonebot_plugin_xiuxian_2.plugin import apply_platform_schema


class AuctionBidWebReconcileTests(unittest.TestCase):
    def test_direct_web_application_uses_same_bid_effects(self):
        with tempfile.TemporaryDirectory(prefix="auction-web-bid-") as directory:
            context = build_runtime_context(data_dir=directory, legacy_startup=False)
            game_db = context.database.path("game_db")
            player_db = context.database.path("player_db")
            with DatabaseUnitOfWork(game_db) as uow:
                uow.execute("CREATE TABLE user_xiuxian (user_id TEXT PRIMARY KEY, stone INTEGER)")
                uow.execute("INSERT INTO user_xiuxian VALUES (?, ?)", ("u1", 500))
                apply_platform_schema(uow)
                apply_auction_settlement(uow)
                apply_auction_bid_operations(uow)
                uow.execute(
                    "INSERT INTO auction_current "
                    "(id,item_id,name,start_price,current_price,seller_id,seller_name,bids,bid_times,is_system,last_bid_time) "
                    "VALUES (?,?,?,?,?,?,?,?,?,?,?)",
                    ("a1", 1, "灵剑", 100, 200, "seller", "卖家", "{}", "{}", 0, 1),
                )
            with DatabaseUnitOfWork(player_db) as uow:
                apply_auction_bid_statistics(uow)

            client = create_app(context=context).test_client()
            csrf = client.get("/api/v1/csrf").get_json()["data"]["token"]
            response = client.post(
                "/api/v1/auction/bids",
                headers={"X-User-ID": "u1", "X-CSRF-Token": csrf, "Idempotency-Key": "web-bid-1"},
                json={
                    "auction_id": "a1",
                    "bidder_id": "u1",
                    "bid_price": 300,
                    "expected_price": 200,
                    "expected_bids": {},
                    "bid_time": 2,
                },
            )

            self.assertEqual(response.status_code, 200)
            with DatabaseUnitOfWork(player_db) as uow:
                stats = uow.query_one(
                    'SELECT "拍卖出价次数","拍卖出价灵石" FROM statistics WHERE user_id=?',
                    ("u1",),
                )
            log_file = context.paths.players / "u1" / "logs" / "260924.log"
            self.assertEqual((stats["拍卖出价次数"], stats["拍卖出价灵石"]), (1, 300))
            self.assertTrue(log_file.exists())

    def test_admin_reconcile_dispatches_bid_effect_outbox(self):
        with tempfile.TemporaryDirectory(prefix="auction-web-reconcile-") as directory:
            context = build_runtime_context(data_dir=directory, legacy_startup=False)
            game_db = context.database.path("game_db")
            player_db = context.database.path("player_db")
            with DatabaseUnitOfWork(game_db) as uow:
                apply_platform_schema(uow)
                OutboxStore().append(
                    uow,
                    event_id="bid-1:auction.bid",
                    aggregate_type="auction",
                    aggregate_id="a1",
                    event_type=AuctionBidApplication.effects_event,
                    payload={
                        "operation_id": "bid-1",
                        "auction_id": "a1",
                        "bidder_id": "u1",
                        "item_name": "灵剑",
                        "bid_price": 300,
                        "occurred_at": "2026-09-24T10:00:00+00:00",
                    },
                )
            with DatabaseUnitOfWork(player_db) as uow:
                apply_auction_bid_statistics(uow)

            application = AuctionBidApplication(
                game_db,
                effects=LegacyAuctionBidEffects(player_db),
            )
            context.outbox_handlers = {
                AuctionBidApplication.effects_event: application.reconcile_outbox_event,
            }
            context.reconcile_handlers = {}
            client = create_app(context=context).test_client()
            csrf = client.get("/api/v1/csrf").get_json()["data"]["token"]
            response = client.post(
                "/api/v1/reconcile",
                headers={"X-Role": "admin", "X-CSRF-Token": csrf},
            )

            self.assertEqual(response.status_code, 200)
            self.assertTrue(response.get_json()["data"]["clean"])
            with DatabaseUnitOfWork(game_db) as uow:
                row = uow.query_one("SELECT status FROM domain_outbox WHERE event_id='bid-1:auction.bid'")
            with DatabaseUnitOfWork(player_db) as uow:
                stats = uow.query_one(
                    'SELECT "拍卖出价次数","拍卖出价灵石" FROM statistics WHERE user_id=?',
                    ("u1",),
                )
            log_file = context.paths.players / "u1" / "logs" / "260924.log"
            self.assertEqual(row["status"], "sent")
            self.assertEqual((stats["拍卖出价次数"], stats["拍卖出价灵石"]), (1, 300))
            self.assertEqual(log_file.exists(), True)

            with DatabaseUnitOfWork(game_db) as uow:
                OutboxStore().append(
                    uow,
                    event_id="bid-2:auction.bid",
                    aggregate_type="auction",
                    aggregate_id="a1",
                    event_type=AuctionBidApplication.effects_event,
                    payload={
                        "operation_id": "bid-2",
                        "auction_id": "a1",
                        "bidder_id": "u1",
                        "item_name": "灵剑",
                        "bid_price": 400,
                        "occurred_at": "2026-09-24T10:01:00+00:00",
                    },
                )
            from nonebot_plugin_xiuxian_2.cli import main as cli_main

            with redirect_stdout(StringIO()):
                self.assertEqual(cli_main(["reconcile", "--apply", "--data-dir", directory]), 0)
            with DatabaseUnitOfWork(player_db) as uow:
                stats = uow.query_one(
                    'SELECT "拍卖出价次数","拍卖出价灵石" FROM statistics WHERE user_id=?',
                    ("u1",),
                )
            self.assertEqual((stats["拍卖出价次数"], stats["拍卖出价灵石"]), (2, 700))


if __name__ == "__main__":
    unittest.main()
