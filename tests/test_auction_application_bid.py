import tempfile
import unittest
from pathlib import Path

import nonebot
nonebot.init()

from nonebot_plugin_xiuxian_2.features.auction.application import AuctionBidApplication
from nonebot_plugin_xiuxian_2.infrastructure.database import DatabaseUnitOfWork
from nonebot_plugin_xiuxian_2.plugin import apply_platform_schema
from nonebot_plugin_xiuxian_2.features.auction.migrations import apply_auction_bid_operations
from nonebot_plugin_xiuxian_2.xiuxian.xiuxian_trade.repository import TradeRepository
from tests.test_db_backend import db_backend


class AuctionApplicationBidTests(unittest.TestCase):
    def test_default_bid_uses_feature_repository_and_replays(self):
        with tempfile.TemporaryDirectory() as temp:
            database = Path(temp) / "game.db"
            with db_backend.transaction(database) as conn:
                conn.execute("CREATE TABLE user_xiuxian(user_id TEXT PRIMARY KEY, stone INTEGER)")
                conn.executemany("INSERT INTO user_xiuxian VALUES(?,?)", (("old", 50), ("u", 500)))
            with DatabaseUnitOfWork(database) as uow:
                apply_platform_schema(uow)
                apply_auction_bid_operations(uow)
            repository = TradeRepository(database, max_goods_num=1000)
            repository.initialize()
            repository.set_current_auction([{
                "id": "a",
                "item_id": 1,
                "name": "item",
                "start_price": 100,
                "current_price": 200,
                "seller_id": "seller",
                "seller_name": "seller",
                "bids": {"old": 200},
                "bid_times": {},
                "is_system": False,
                "last_bid_time": 1,
            }])
            application = AuctionBidApplication(database)
            kwargs = {
                "operation_id": "bid-1",
                "auction_id": "a",
                "bidder_id": "u",
                "bid_price": 300,
                "expected_price": 200,
                "expected_bids": {"old": 200},
                "bid_time": 2.0,
            }
            first = application.place_bid(**kwargs)
            replay = application.place_bid(**kwargs)
            self.assertEqual((first.status, replay.status), ("applied", "replayed"))
            with db_backend.connection(database) as conn:
                balances = dict(conn.execute("SELECT user_id,stone FROM user_xiuxian"))
                self.assertEqual(balances, {"old": 250, "u": 200})
                self.assertEqual(
                    conn.execute("SELECT current_price FROM auction_current WHERE id='a'").fetchone()[0],
                    300,
                )
