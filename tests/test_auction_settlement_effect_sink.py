from __future__ import annotations

import tempfile
import unittest
from pathlib import Path

from nonebot_plugin_xiuxian_2.features.auction.migrations import apply_auction_settlement_statistics
from nonebot_plugin_xiuxian_2.features.auction.settlement_statistics import AuctionSettlementStatisticsRepository
from nonebot_plugin_xiuxian_2.compatibility.auction_settlement_effects import LegacyAuctionSettlementEffects
from nonebot_plugin_xiuxian_2.infrastructure.database import DatabaseUnitOfWork


class AuctionSettlementEffectSinkTests(unittest.TestCase):
    def test_winner_seller_and_miss_projections_are_idempotent(self):
        with tempfile.TemporaryDirectory() as directory:
            database = Path(directory) / "player.db"
            with DatabaseUnitOfWork(database) as uow:
                apply_auction_settlement_statistics(uow)
            logs = {}
            game_events = {}

            def write_log(**event):
                logs.setdefault(event["event_id"], event)

            def write_game_event(user_id, event_key, amount, meta):
                event_id = meta["event_id"]
                if event_id not in game_events:
                    game_events[event_id] = (user_id, event_key, amount, dict(meta))
                return {"economy_log_id": len(game_events), "season_rank": [{}, {}, {}]}

            effects = LegacyAuctionSettlementEffects(
                database,
                statistics=AuctionSettlementStatisticsRepository(database),
                log_writer=write_log,
                game_event_writer=write_game_event,
            )
            events = [
                {
                    "event_id": "settle-1:a-1:winner",
                    "event_key": "winner",
                    "operation_id": "settle-1",
                    "settlement": {
                        "auction_id": "a-1", "item_id": 10, "item_name": "灵剑",
                        "winner_id": "buyer", "seller_id": "seller", "final_price": 300,
                    },
                    "occurred_at": "2026-09-24T10:00:00+00:00",
                    "replayed": False,
                },
                {
                    "event_id": "settle-1:a-1:seller",
                    "event_key": "seller",
                    "operation_id": "settle-1",
                    "settlement": {
                        "auction_id": "a-1", "item_id": 10, "item_name": "灵剑",
                        "winner_id": "buyer", "seller_id": "seller", "final_price": 300,
                        "fee": 30, "seller_earnings": 270,
                    },
                    "occurred_at": "2026-09-24T10:00:00+00:00",
                    "replayed": False,
                },
                {
                    "event_id": "settle-1:a-2:seller_miss",
                    "event_key": "seller_miss",
                    "operation_id": "settle-1",
                    "settlement": {
                        "auction_id": "a-2", "item_id": 11, "item_name": "灵草",
                        "seller_id": "seller-2", "final_price": None, "status": "流拍",
                    },
                    "occurred_at": "2026-09-24T10:00:00+00:00",
                    "replayed": False,
                },
            ]

            for event in events:
                effects.on_settlement(**event)
                effects.on_settlement(**{**event, "replayed": True})

            with DatabaseUnitOfWork(database) as uow:
                buyer = uow.query_one(
                    'SELECT "拍卖成交次数","拍卖消费灵石","交易购买","拍卖成交" '
                    "FROM statistics WHERE user_id='buyer'"
                )
                seller = uow.query_one(
                    'SELECT "拍卖售出次数","拍卖收入灵石","拍卖手续费消耗","交易出售","拍卖成交" '
                    "FROM statistics WHERE user_id='seller'"
                )
                miss = uow.query_one(
                    'SELECT "拍卖流拍次数" FROM statistics WHERE user_id=?', ("seller-2",)
                )
                projected = uow.query_one(
                    "SELECT COUNT(*) AS n FROM auction_settlement_statistics_events"
                )["n"]

            self.assertEqual(tuple(buyer.values()), (1, 300, 1, 1))
            self.assertEqual(tuple(seller.values()), (1, 270, 30, 1, 1))
            self.assertEqual(miss["拍卖流拍次数"], 1)
            self.assertEqual(projected, 10)
            self.assertEqual(len(logs), 3)
            self.assertEqual(len(game_events), 2)
            self.assertEqual(
                {item[1] for item in game_events.values()}, {"trade_buy", "trade_sell"}
            )
            self.assertTrue(all(item[3]["skip_statistics"] for item in game_events.values()))


if __name__ == "__main__":
    unittest.main()
