from __future__ import annotations

import json
import tempfile
import unittest
from pathlib import Path

import nonebot

nonebot.init()

from nonebot_plugin_xiuxian_2.xiuxian.xiuxian_trade.repository import TradeRepository
from tests.test_db_backend import db_backend


class GuishiOrderMatchingTests(unittest.TestCase):
    def setUp(self) -> None:
        self.temp_dir = tempfile.TemporaryDirectory()
        root = Path(self.temp_dir.name)
        self.game_database = root / "game.sqlite3"
        self.trade_database = root / "trade.sqlite3"
        with db_backend.transaction(self.trade_database) as conn:
            conn.execute(
                """
                CREATE TABLE guishi_item (
                    id TEXT PRIMARY KEY,
                    user_id TEXT,
                    item_id INTEGER,
                    item_name TEXT,
                    item_type TEXT,
                    price INTEGER,
                    quantity INTEGER,
                    filled_quantity INTEGER DEFAULT 0
                )
                """
            )
            conn.execute(
                """
                CREATE TABLE guishi_info (
                    user_id TEXT PRIMARY KEY,
                    stored_stone INTEGER DEFAULT 0,
                    items TEXT DEFAULT '{}'
                )
                """
            )
            conn.executemany(
                "INSERT INTO guishi_item VALUES (%s, %s, %s, %s, %s, %s, %s, %s)",
                (
                    ("want", "buyer", 1001, "灵草", "qiugou", 30, 8, 2),
                    ("sell", "seller", 1001, "灵草", "baitan", 20, 10, 6),
                ),
            )
            conn.execute(
                "INSERT INTO guishi_info VALUES (%s, %s, %s)",
                ("buyer", 99, '{"1002": 1}'),
            )
            conn.execute(
                "INSERT INTO guishi_info VALUES (%s, %s, %s)",
                ("seller", 17, '{}'),
            )
        self.repository = TradeRepository(self.game_database, max_goods_num=99)

    def tearDown(self) -> None:
        self.temp_dir.cleanup()

    def order(self, order_id):
        with db_backend.connection(self.trade_database) as conn:
            return conn.execute(
                "SELECT quantity, filled_quantity FROM guishi_item WHERE id=%s", (order_id,)
            ).fetchone()

    def info(self, user_id):
        with db_backend.connection(self.trade_database) as conn:
            row = conn.execute(
                "SELECT stored_stone, items FROM guishi_info WHERE user_id=%s", (user_id,)
            ).fetchone()
            return int(row[0]), json.loads(row[1])

    def test_match_updates_both_accounts_and_orders_atomically(self) -> None:
        with db_backend.transaction(self.trade_database) as conn:
            conn.execute("UPDATE guishi_item SET quantity=%s WHERE id=%s", (12, "sell"))
        result = self.repository.match_guishi_orders(self.trade_database, "want", "sell")

        self.assertTrue(result.matched)
        self.assertEqual(result.quantity, 6)
        self.assertEqual(result.amount, 120)
        self.assertTrue(result.qiugou_completed)
        self.assertTrue(result.baitan_completed)
        self.assertIsNone(self.order("want"))
        self.assertIsNone(self.order("sell"))
        self.assertEqual(self.info("buyer"), (99, {"1001": 6, "1002": 1}))
        self.assertEqual(self.info("seller"), (137, {}))

    def test_partial_match_increments_filled_quantities(self) -> None:
        with db_backend.transaction(self.trade_database) as conn:
            conn.execute("UPDATE guishi_item SET quantity=%s WHERE id=%s", (12, "want"))

        result = self.repository.match_guishi_orders(self.trade_database, "want", "sell")

        self.assertTrue(result.matched)
        self.assertFalse(result.qiugou_completed)
        self.assertTrue(result.baitan_completed)
        self.assertEqual(tuple(self.order("want")), (12, 6))
        self.assertIsNone(self.order("sell"))
        self.assertEqual(self.info("buyer")[1]["1001"], 4)
        self.assertEqual(self.info("seller")[0], 97)

    def test_account_write_failure_rolls_back_orders_and_other_account(self) -> None:
        with db_backend.transaction(self.trade_database) as conn:
            conn.execute(
                """
                CREATE TRIGGER reject_seller_update
                BEFORE UPDATE ON guishi_info WHEN NEW.user_id='seller'
                BEGIN
                    SELECT RAISE(ABORT, 'reject seller');
                END
                """
            )

        with self.assertRaises(db_backend.IntegrityError):
            self.repository.match_guishi_orders(self.trade_database, "want", "sell")

        self.assertEqual(tuple(self.order("want")), (8, 2))
        self.assertEqual(tuple(self.order("sell")), (10, 6))
        self.assertEqual(self.info("buyer"), (99, {"1002": 1}))
        self.assertEqual(self.info("seller"), (17, {}))

    def test_invalid_price_or_self_trade_does_not_change_state(self) -> None:
        with db_backend.transaction(self.trade_database) as conn:
            conn.execute("UPDATE guishi_item SET price=%s WHERE id=%s", (31, "sell"))

        result = self.repository.match_guishi_orders(self.trade_database, "want", "sell")

        self.assertEqual(result.status, "not_matchable")
        self.assertEqual(tuple(self.order("want")), (8, 2))
        self.assertEqual(tuple(self.order("sell")), (10, 6))
        self.assertEqual(self.info("seller"), (17, {}))

    def test_completed_order_is_removed_without_crediting_assets(self) -> None:
        with db_backend.transaction(self.trade_database) as conn:
            conn.execute("UPDATE guishi_item SET filled_quantity=quantity WHERE id=%s", ("want",))

        result = self.repository.match_guishi_orders(self.trade_database, "want", "sell")

        self.assertEqual(result.status, "qiugou_completed")
        self.assertIsNone(self.order("want"))
        self.assertEqual(tuple(self.order("sell")), (10, 6))
        self.assertEqual(self.info("buyer"), (99, {"1002": 1}))


if __name__ == "__main__":
    unittest.main()
