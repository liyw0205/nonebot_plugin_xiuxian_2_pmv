from __future__ import annotations

import tempfile
import unittest
from pathlib import Path

import nonebot

nonebot.init()

from nonebot_plugin_xiuxian_2.xiuxian.xiuxian_trade.repository import (
    TradeRepository,
)
from tests.test_db_backend import db_backend


class AuctionSettlementTests(unittest.TestCase):
    def setUp(self) -> None:
        self.temp_dir = tempfile.TemporaryDirectory()
        self.database = Path(self.temp_dir.name) / "auction-settlement.sqlite3"
        with db_backend.transaction(self.database) as conn:
            conn.execute(
                "CREATE TABLE user_xiuxian (user_id TEXT PRIMARY KEY, stone INTEGER NOT NULL)"
            )
            conn.execute(
                """
                CREATE TABLE back (
                    user_id TEXT NOT NULL,
                    goods_id INTEGER NOT NULL,
                    goods_name TEXT,
                    goods_type TEXT,
                    goods_num INTEGER DEFAULT 0,
                    create_time TEXT,
                    update_time TEXT,
                    bind_num INTEGER DEFAULT 0,
                    UNIQUE (user_id, goods_id)
                )
                """
            )
            conn.executemany(
                "INSERT INTO user_xiuxian VALUES (%s, %s)",
                (("seller", 100), ("winner", 0), ("old-bidder", 0)),
            )
        self.repository = TradeRepository(self.database, max_goods_num=99)
        self.repository.initialize()

    def tearDown(self) -> None:
        self.temp_dir.cleanup()

    def add_auction(self, *, auction_id="auction-1", bids=None, is_system=False):
        self.repository.set_current_auction(
            [
                {
                    "id": auction_id,
                    "item_id": 5001,
                    "name": "测试拍品",
                    "start_price": 100,
                    "current_price": 300,
                    "seller_id": "0" if is_system else "seller",
                    "seller_name": "系统" if is_system else "卖家",
                    "bids": bids or {},
                    "bid_times": {},
                    "is_system": is_system,
                    "last_bid_time": 1.0,
                }
            ]
        )

    def scalar(self, sql, params=()):
        with db_backend.connection(self.database) as conn:
            row = conn.execute(sql, params).fetchone()
            return row[0] if row else None

    def test_sold_item_settles_all_assets_history_and_removal_atomically(self) -> None:
        self.add_auction(bids={"winner": 300, "old-bidder": 200})

        result = self.repository.settle_auction_item(
            "auction-1",
            item_type="装备",
            winner_name="买家",
            fee_rate=0.1,
            end_time=2.0,
        )

        self.assertEqual(result.status, "sold")
        self.assertEqual(result.fee, 30)
        self.assertEqual(result.seller_earnings, 270)
        self.assertEqual(
            self.scalar("SELECT stone FROM user_xiuxian WHERE user_id=%s", ("seller",)),
            370,
        )
        self.assertEqual(
            self.scalar("SELECT stone FROM user_xiuxian WHERE user_id=%s", ("old-bidder",)),
            200,
        )
        self.assertEqual(
            self.scalar("SELECT goods_num FROM back WHERE user_id=%s", ("winner",)),
            1,
        )
        self.assertIsNone(self.repository.get_current_auction("auction-1"))
        self.assertEqual(
            self.repository.get_auction_history("auction-1")[0]["status"], "成交"
        )

    def test_duplicate_settlement_does_not_apply_assets_twice(self) -> None:
        self.add_auction(bids={"winner": 300})
        first = self.repository.settle_auction_item(
            "auction-1", item_type="装备", winner_name="买家", fee_rate=0.1, end_time=2.0
        )
        second = self.repository.settle_auction_item(
            "auction-1", item_type="装备", winner_name="买家", fee_rate=0.1, end_time=3.0
        )

        self.assertTrue(first.applied)
        self.assertEqual(second.status, "duplicate")
        self.assertEqual(
            self.scalar("SELECT stone FROM user_xiuxian WHERE user_id=%s", ("seller",)),
            370,
        )
        self.assertEqual(
            self.scalar("SELECT goods_num FROM back WHERE user_id=%s", ("winner",)),
            1,
        )
        self.assertEqual(len(self.repository.get_auction_history("auction-1")), 1)

    def test_unsold_player_item_is_returned_in_same_transaction(self) -> None:
        self.add_auction()

        result = self.repository.settle_auction_item(
            "auction-1", item_type="装备", winner_name=None, fee_rate=0.1, end_time=2.0
        )

        self.assertEqual(result.status, "unsold")
        self.assertEqual(
            self.scalar("SELECT goods_num FROM back WHERE user_id=%s", ("seller",)),
            1,
        )
        self.assertEqual(
            self.repository.get_auction_history("auction-1")[0]["status"], "流拍"
        )

    def test_failure_rolls_back_assets_history_operation_and_current_item(self) -> None:
        self.add_auction(bids={"winner": 300})
        with db_backend.transaction(self.database) as conn:
            conn.execute(
                """
                CREATE TRIGGER fail_settlement BEFORE INSERT ON auction_settlement_operations
                BEGIN SELECT RAISE(ABORT, 'settlement failed'); END
                """
            )

        with self.assertRaises(db_backend.IntegrityError):
            self.repository.settle_auction_item(
                "auction-1", item_type="装备", winner_name="买家", fee_rate=0.1, end_time=2.0
            )

        self.assertEqual(
            self.scalar("SELECT stone FROM user_xiuxian WHERE user_id=%s", ("seller",)),
            100,
        )
        self.assertIsNone(self.scalar("SELECT goods_num FROM back WHERE user_id=%s", ("winner",)))
        self.assertEqual(self.repository.get_auction_history("auction-1"), [])
        self.assertIsNotNone(self.repository.get_current_auction("auction-1"))

    def test_missing_item_metadata_keeps_player_auction_pending(self) -> None:
        self.add_auction(bids={"winner": 300})

        result = self.repository.settle_auction_item(
            "auction-1", item_type=None, winner_name="买家", fee_rate=0.1, end_time=2.0
        )

        self.assertEqual(result.status, "item_missing")
        self.assertIsNotNone(self.repository.get_current_auction("auction-1"))
        self.assertEqual(self.repository.get_auction_history("auction-1"), [])

    def test_full_inventory_keeps_auction_pending_without_partial_payout(self) -> None:
        self.add_auction(bids={"winner": 300})
        with db_backend.transaction(self.database) as conn:
            conn.execute(
                "INSERT INTO back (user_id, goods_id, goods_num, bind_num) VALUES (%s, %s, %s, %s)",
                ("winner", 5001, 99, 99),
            )

        result = self.repository.settle_auction_item(
            "auction-1", item_type="装备", winner_name="买家", fee_rate=0.1, end_time=2.0
        )

        self.assertEqual(result.status, "inventory_full")
        self.assertEqual(
            self.scalar("SELECT stone FROM user_xiuxian WHERE user_id=%s", ("seller",)),
            100,
        )
        self.assertIsNotNone(self.repository.get_current_auction("auction-1"))
        self.assertEqual(self.repository.get_auction_history("auction-1"), [])


if __name__ == "__main__":
    unittest.main()
