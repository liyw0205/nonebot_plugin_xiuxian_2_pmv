from __future__ import annotations

import tempfile
import unittest
from pathlib import Path

import nonebot

nonebot.init()

from nonebot_plugin_xiuxian_2.xiuxian.xiuxian_trade.repository import TradeRepository
from tests.test_db_backend import db_backend


class GuishiExpiredOrderCleanupTests(unittest.TestCase):
    def setUp(self) -> None:
        self.temp_dir = tempfile.TemporaryDirectory()
        root = Path(self.temp_dir.name)
        self.game_database = root / "game.sqlite3"
        self.trade_database = root / "trade.sqlite3"
        with db_backend.transaction(self.game_database) as conn:
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
                "INSERT INTO guishi_item VALUES (%s, %s, %s, %s, %s, %s, %s, %s)",
                ("order-1", "seller", 1001, "灵草", "baitan", 20, 10, 4),
            )
        self.repository = TradeRepository(self.game_database, max_goods_num=10)

    def tearDown(self) -> None:
        self.temp_dir.cleanup()

    def inventory(self):
        with db_backend.connection(self.game_database) as conn:
            row = conn.execute(
                "SELECT goods_num FROM back WHERE user_id=%s AND goods_id=%s",
                ("seller", 1001),
            ).fetchone()
            return None if row is None else int(row[0])

    def order_exists(self, order_id="order-1") -> bool:
        with db_backend.connection(self.trade_database) as conn:
            return bool(
                conn.execute(
                    "SELECT 1 FROM guishi_item WHERE id=%s", (order_id,)
                ).fetchone()
            )

    def test_refund_and_order_removal_commit_together(self) -> None:
        result = self.repository.clear_expired_guishi_order(
            self.trade_database, "order-1", "药材"
        )

        self.assertEqual(result.status, "cleared")
        self.assertEqual(result.refunded_quantity, 6)
        self.assertEqual(self.inventory(), 6)
        self.assertFalse(self.order_exists())
        with db_backend.connection(self.game_database) as conn:
            self.assertEqual(
                conn.execute(
                    "SELECT goods_type FROM back WHERE user_id=%s AND goods_id=%s",
                    ("seller", 1001),
                ).fetchone()[0],
                "药材",
            )

    def test_existing_inventory_is_incremented(self) -> None:
        with db_backend.transaction(self.game_database) as conn:
            conn.execute(
                "INSERT INTO back VALUES (%s, %s, %s, %s, %s, NULL, NULL, %s)",
                ("seller", 1001, "灵草", "药材", 3, 0),
            )

        result = self.repository.clear_expired_guishi_order(
            self.trade_database, "order-1", "药材"
        )

        self.assertTrue(result.cleared)
        self.assertEqual(self.inventory(), 9)
        self.assertFalse(self.order_exists())

    def test_full_inventory_keeps_order_for_later_retry(self) -> None:
        with db_backend.transaction(self.game_database) as conn:
            conn.execute(
                "INSERT INTO back VALUES (%s, %s, %s, %s, %s, NULL, NULL, %s)",
                ("seller", 1001, "灵草", "药材", 5, 0),
            )

        result = self.repository.clear_expired_guishi_order(
            self.trade_database, "order-1", "药材"
        )

        self.assertEqual(result.status, "inventory_full")
        self.assertEqual(self.inventory(), 5)
        self.assertTrue(self.order_exists())

    def test_delete_failure_rolls_back_refund(self) -> None:
        with db_backend.transaction(self.trade_database) as conn:
            conn.execute(
                """
                CREATE TRIGGER reject_order_delete
                BEFORE DELETE ON guishi_item
                BEGIN
                    SELECT RAISE(ABORT, 'reject delete');
                END
                """
            )

        with self.assertRaises(db_backend.IntegrityError):
            self.repository.clear_expired_guishi_order(
                self.trade_database, "order-1", "药材"
            )

        self.assertIsNone(self.inventory())
        self.assertTrue(self.order_exists())

    def test_missing_order_is_idempotent(self) -> None:
        self.repository.clear_expired_guishi_order(
            self.trade_database, "order-1", "药材"
        )
        result = self.repository.clear_expired_guishi_order(
            self.trade_database, "order-1", "药材"
        )

        self.assertEqual(result.status, "order_missing")
        self.assertEqual(self.inventory(), 6)

    def test_non_baitan_order_is_not_modified(self) -> None:
        with db_backend.transaction(self.trade_database) as conn:
            conn.execute(
                "UPDATE guishi_item SET item_type=%s WHERE id=%s",
                ("qiugou", "order-1"),
            )

        result = self.repository.clear_expired_guishi_order(
            self.trade_database, "order-1", "药材"
        )

        self.assertEqual(result.status, "not_baitan")
        self.assertIsNone(self.inventory())
        self.assertTrue(self.order_exists())

    def test_qiugou_clear_refunds_stone_and_removes_order_atomically(self) -> None:
        with db_backend.transaction(self.trade_database) as conn:
            conn.execute(
                "UPDATE guishi_item SET item_type=%s, price=%s, quantity=%s, filled_quantity=%s WHERE id=%s",
                ("qiugou", 20, 10, 4, "order-1"),
            )
            conn.execute(
                "CREATE TABLE guishi_info (user_id TEXT PRIMARY KEY, stored_stone INTEGER, items TEXT)"
            )
            conn.execute(
                "INSERT INTO guishi_info VALUES (%s, %s, %s)",
                ("seller", 30, '{}'),
            )

        result = self.repository.clear_guishi_qiugou_order(
            self.trade_database, "order-1"
        )

        self.assertTrue(result.cleared)
        self.assertEqual(result.refunded_stone, 120)
        self.assertFalse(self.order_exists())
        with db_backend.connection(self.trade_database) as conn:
            self.assertEqual(
                conn.execute(
                    "SELECT stored_stone FROM guishi_info WHERE user_id=%s", ("seller",)
                ).fetchone()[0],
                150,
            )

    def test_qiugou_delete_failure_rolls_back_stone_refund(self) -> None:
        with db_backend.transaction(self.trade_database) as conn:
            conn.execute(
                "UPDATE guishi_item SET item_type=%s, price=%s, quantity=%s, filled_quantity=%s WHERE id=%s",
                ("qiugou", 20, 10, 4, "order-1"),
            )
            conn.execute(
                "CREATE TABLE guishi_info (user_id TEXT PRIMARY KEY, stored_stone INTEGER, items TEXT)"
            )
            conn.execute(
                "INSERT INTO guishi_info VALUES (%s, %s, %s)",
                ("seller", 30, '{}'),
            )
            conn.execute(
                """
                CREATE TRIGGER reject_qiugou_delete
                BEFORE DELETE ON guishi_item
                BEGIN
                    SELECT RAISE(ABORT, 'reject delete');
                END
                """
            )

        with self.assertRaises(db_backend.IntegrityError):
            self.repository.clear_guishi_qiugou_order(self.trade_database, "order-1")

        self.assertTrue(self.order_exists())
        with db_backend.connection(self.trade_database) as conn:
            self.assertEqual(
                conn.execute(
                    "SELECT stored_stone FROM guishi_info WHERE user_id=%s", ("seller",)
                ).fetchone()[0],
                30,
            )


if __name__ == "__main__":
    unittest.main()
