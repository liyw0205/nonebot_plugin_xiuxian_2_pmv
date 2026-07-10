from __future__ import annotations

import tempfile
import unittest
from pathlib import Path

import nonebot

nonebot.init()

from tests.test_db_backend import db_backend

from nonebot_plugin_xiuxian_2.xiuxian.xiuxian_trade.repository import TradeRepository


class TradePurchaseTests(unittest.TestCase):
    def setUp(self) -> None:
        self.temp_dir = tempfile.TemporaryDirectory()
        self.database = Path(self.temp_dir.name) / "trade-flow.sqlite3"
        with db_backend.transaction(self.database) as conn:
            conn.execute(
                """
                CREATE TABLE user_xiuxian (
                    user_id TEXT PRIMARY KEY,
                    stone INTEGER NOT NULL
                )
                """
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
            conn.execute("INSERT INTO user_xiuxian VALUES (%s, %s)", ("buyer", 1000))
            conn.execute("INSERT INTO user_xiuxian VALUES (%s, %s)", ("seller", 100))
        self.repository = TradeRepository(self.database, max_goods_num=99)
        self.repository.initialize()
        self.listing_id = self.repository.add_xianshi_item(
            "seller", 1001, "测试法器", "装备", 200, 3
        )

    def tearDown(self) -> None:
        self.temp_dir.cleanup()

    def scalar(self, sql, params=()):
        with db_backend.connection(self.database) as conn:
            row = conn.execute(sql, params).fetchone()
            return row[0] if row else None

    def assert_unchanged(self) -> None:
        self.assertEqual(
            self.scalar("SELECT stone FROM user_xiuxian WHERE user_id=%s", ("buyer",)),
            1000,
        )
        self.assertEqual(
            self.scalar("SELECT stone FROM user_xiuxian WHERE user_id=%s", ("seller",)),
            100,
        )
        self.assertEqual(
            self.scalar("SELECT quantity FROM xianshi_item WHERE id=%s", (self.listing_id,)),
            3,
        )
        self.assertIsNone(
            self.scalar("SELECT goods_num FROM back WHERE user_id=%s", ("buyer",))
        )

    def test_purchase_updates_all_assets_in_one_transaction(self) -> None:
        result = self.repository.purchase_xianshi_item(
            "event-1", "buyer", self.listing_id, 2
        )

        self.assertTrue(result.applied)
        self.assertEqual(result.total_cost, 400)
        self.assertEqual(
            self.scalar("SELECT stone FROM user_xiuxian WHERE user_id=%s", ("buyer",)),
            600,
        )
        self.assertEqual(
            self.scalar("SELECT stone FROM user_xiuxian WHERE user_id=%s", ("seller",)),
            500,
        )
        self.assertEqual(
            self.scalar("SELECT quantity FROM xianshi_item WHERE id=%s", (self.listing_id,)),
            1,
        )
        self.assertEqual(
            self.scalar(
                "SELECT goods_num FROM back WHERE user_id=%s AND goods_id=%s",
                ("buyer", 1001),
            ),
            2,
        )

    def test_repeated_operation_does_not_charge_or_deliver_twice(self) -> None:
        first = self.repository.purchase_xianshi_item(
            "event-repeat", "buyer", self.listing_id, 1
        )
        second = self.repository.purchase_xianshi_item(
            "event-repeat", "buyer", self.listing_id, 1
        )

        self.assertTrue(first.applied)
        self.assertEqual(second.status, "duplicate")
        self.assertFalse(second.applied)
        self.assertEqual(
            self.scalar("SELECT stone FROM user_xiuxian WHERE user_id=%s", ("buyer",)),
            800,
        )
        self.assertEqual(
            self.scalar("SELECT goods_num FROM back WHERE user_id=%s", ("buyer",)),
            1,
        )
        self.assertEqual(
            self.scalar("SELECT quantity FROM xianshi_item WHERE id=%s", (self.listing_id,)),
            2,
        )

    def test_insufficient_balance_and_missing_seller_leave_no_partial_state(self) -> None:
        with db_backend.transaction(self.database) as conn:
            conn.execute(
                "UPDATE user_xiuxian SET stone=%s WHERE user_id=%s", (100, "buyer")
            )
        result = self.repository.purchase_xianshi_item(
            "event-poor", "buyer", self.listing_id, 1
        )
        self.assertEqual(result.status, "stone_insufficient")
        with db_backend.transaction(self.database) as conn:
            conn.execute(
                "UPDATE user_xiuxian SET stone=%s WHERE user_id=%s", (1000, "buyer")
            )
            conn.execute("DELETE FROM user_xiuxian WHERE user_id=%s", ("seller",))
        result = self.repository.purchase_xianshi_item(
            "event-missing-seller", "buyer", self.listing_id, 1
        )
        self.assertEqual(result.status, "seller_missing")
        self.assertEqual(
            self.scalar("SELECT stone FROM user_xiuxian WHERE user_id=%s", ("buyer",)),
            1000,
        )
        self.assertEqual(
            self.scalar("SELECT quantity FROM xianshi_item WHERE id=%s", (self.listing_id,)),
            3,
        )
        self.assertIsNone(
            self.scalar("SELECT goods_num FROM back WHERE user_id=%s", ("buyer",))
        )

    def test_delivery_database_error_rolls_back_charge_stock_and_income(self) -> None:
        with db_backend.transaction(self.database) as conn:
            conn.execute(
                """
                CREATE TRIGGER fail_delivery BEFORE INSERT ON back
                BEGIN SELECT RAISE(ABORT, 'delivery failed'); END
                """
            )

        with self.assertRaises(db_backend.IntegrityError):
            self.repository.purchase_xianshi_item(
                "event-failure", "buyer", self.listing_id, 1
            )

        self.assert_unchanged()
        self.assertEqual(
            self.scalar(
                "SELECT COUNT(*) FROM xianshi_operations WHERE operation_id=%s",
                ("event-failure",),
            ),
            0,
        )

    def test_inventory_limit_rejects_purchase_without_mutation(self) -> None:
        with db_backend.transaction(self.database) as conn:
            conn.execute(
                "INSERT INTO back (user_id, goods_id, goods_num, bind_num) VALUES (%s, %s, %s, %s)",
                ("buyer", 1001, 99, 0),
            )

        result = self.repository.purchase_xianshi_item(
            "event-full", "buyer", self.listing_id, 1
        )

        self.assertEqual(result.status, "inventory_full")
        self.assertEqual(
            self.scalar("SELECT stone FROM user_xiuxian WHERE user_id=%s", ("buyer",)),
            1000,
        )
        self.assertEqual(
            self.scalar("SELECT quantity FROM xianshi_item WHERE id=%s", (self.listing_id,)),
            3,
        )

    def test_legacy_listings_are_imported_only_once(self) -> None:
        legacy = Path(self.temp_dir.name) / "legacy-trade.sqlite3"
        with db_backend.transaction(legacy) as conn:
            conn.execute(
                """
                CREATE TABLE xianshi_item (
                    id TEXT PRIMARY KEY, user_id TEXT, goods_id INTEGER,
                    name TEXT, type TEXT, price INTEGER, quantity INTEGER
                )
                """
            )
            conn.execute(
                "INSERT INTO xianshi_item VALUES (%s, %s, %s, %s, %s, %s, %s)",
                ("legacy-1", "seller", 2001, "旧物品", "药材", 10, 1),
            )

        self.repository.initialize(legacy)
        with db_backend.transaction(self.database) as conn:
            conn.execute("DELETE FROM xianshi_item WHERE id=%s", ("legacy-1",))
        self.repository.initialize(legacy)

        self.assertIsNone(
            self.scalar("SELECT quantity FROM xianshi_item WHERE id=%s", ("legacy-1",))
        )

    def test_legacy_auction_state_and_history_are_imported_only_once(self) -> None:
        legacy = Path(self.temp_dir.name) / "legacy-auction.sqlite3"
        with db_backend.transaction(legacy) as conn:
            TradeRepository.ensure_schema(conn)
            conn.execute(
                """
                INSERT INTO auction_current VALUES (
                    %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s
                )
                """,
                (
                    "auction-legacy",
                    3001,
                    "旧拍品",
                    100,
                    150,
                    "seller",
                    "卖家",
                    '{"buyer": 150}',
                    '{"buyer": 1.5}',
                    0,
                    1.5,
                ),
            )
            conn.execute(
                """
                INSERT INTO auction_history (
                    auction_id, item_id, item_name, start_price, final_price,
                    seller_id, seller_name, winner_id, winner_name, status,
                    fee, seller_earnings, start_time, end_time
                ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                """,
                (
                    "history-legacy",
                    3002,
                    "旧成交",
                    100,
                    200,
                    "seller",
                    "卖家",
                    "buyer",
                    "买家",
                    "成交",
                    10,
                    190,
                    1.0,
                    2.0,
                ),
            )

        self.repository.initialize(legacy)
        auction = self.repository.get_current_auction("auction-legacy")
        self.assertEqual(auction["bids"], {"buyer": 150})
        self.assertEqual(auction["bid_times"], {"buyer": 1.5})
        self.assertFalse(auction["is_system"])
        self.assertEqual(
            self.repository.get_auction_history("history-legacy")[0]["final_price"],
            200,
        )

        with db_backend.transaction(self.database) as conn:
            conn.execute(
                "DELETE FROM auction_current WHERE id=%s", ("auction-legacy",)
            )
            conn.execute(
                "DELETE FROM auction_history WHERE auction_id=%s",
                ("history-legacy",),
            )
        self.repository.initialize(legacy)
        self.assertIsNone(self.repository.get_current_auction("auction-legacy"))
        self.assertEqual(
            self.repository.get_auction_history("history-legacy"), []
        )

    def test_current_auction_repository_round_trip_and_compare_and_swap(self) -> None:
        item = {
            "id": "auction-1",
            "item_id": 4001,
            "name": "测试拍品",
            "start_price": 100,
            "current_price": 100,
            "seller_id": "seller",
            "seller_name": "卖家",
            "bids": {},
            "bid_times": {},
            "is_system": False,
            "last_bid_time": 1.0,
        }
        self.repository.set_current_auction([item])

        self.assertTrue(
            self.repository.try_update_auction_bid(
                "auction-1", 100, 150, {"buyer": 150}, {"buyer": 2.0}, 2.0
            )
        )
        self.assertFalse(
            self.repository.try_update_auction_bid(
                "auction-1", 100, 200, {"buyer": 200}, {"buyer": 3.0}, 3.0
            )
        )
        stored = self.repository.get_current_auction("auction-1")
        self.assertEqual(stored["current_price"], 150)
        self.assertEqual(stored["bids"], {"buyer": 150})

        self.repository.clear_current_auction()
        self.assertEqual(self.repository.get_current_auction(), [])


if __name__ == "__main__":
    unittest.main()
