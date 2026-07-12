from __future__ import annotations

import tempfile
import unittest
from pathlib import Path

import nonebot

nonebot.init()

from nonebot_plugin_xiuxian_2.xiuxian.xiuxian_trade.repository import TradeRepository
from tests.test_db_backend import db_backend


class XianshiListingBatchTests(unittest.TestCase):
    def setUp(self) -> None:
        self.temp_dir = tempfile.TemporaryDirectory()
        self.database = Path(self.temp_dir.name) / "xianshi-listing.sqlite3"
        with db_backend.transaction(self.database) as conn:
            TradeRepository.ensure_schema(conn)
            conn.execute(
                "CREATE TABLE user_xiuxian (user_id TEXT PRIMARY KEY, stone INTEGER NOT NULL)"
            )
            conn.execute(
                """
                CREATE TABLE back (
                    user_id TEXT NOT NULL, goods_id INTEGER NOT NULL,
                    goods_name TEXT, goods_type TEXT, goods_num INTEGER DEFAULT 0,
                    bind_num INTEGER DEFAULT 0, state INTEGER DEFAULT 0,
                    update_time TEXT, action_time TEXT,
                    UNIQUE (user_id, goods_id)
                )
                """
            )
        self.repository = TradeRepository(self.database, max_goods_num=99)
        self.repository.initialize()

    def tearDown(self) -> None:
        self.temp_dir.cleanup()

    def scalar(self, sql, params=()):
        with db_backend.connection(self.database) as conn:
            row = conn.execute(sql, params).fetchone()
            return row[0] if row else None

    def test_batch_listing_creates_all_rows_and_records_actual_fee(self) -> None:
        result = self.repository.add_xianshi_items(
            "listing-1", "seller", 1001, "测试法器", "装备", 600000, 3, fee_charged=180000
        )

        self.assertEqual(result.status, "listed")
        self.assertEqual((result.listed_quantity, result.fee_charged, result.fee_refund), (3, 180000, 0))
        self.assertEqual(self.scalar("SELECT COUNT(*) FROM xianshi_item"), 3)
        self.assertEqual(self.scalar("SELECT COUNT(*) FROM xianshi_listing_operations"), 1)

    def test_duplicate_operation_reuses_first_result(self) -> None:
        first = self.repository.add_xianshi_items(
            "listing-repeat", "seller", 1001, "测试法器", "装备", 600000, 2, fee_charged=120000
        )
        second = self.repository.add_xianshi_items(
            "listing-repeat", "seller", 1001, "测试法器", "装备", 600000, 2, fee_charged=200000
        )

        self.assertEqual((first.status, second.status), ("listed", "duplicate"))
        self.assertEqual((second.listed_quantity, second.fee_charged, second.fee_refund), (2, 120000, 0))
        self.assertEqual(self.scalar("SELECT COUNT(*) FROM xianshi_item"), 2)

    def test_insert_failure_rolls_back_all_listings_and_operation(self) -> None:
        with db_backend.transaction(self.database) as conn:
            conn.execute(
                "CREATE TRIGGER fail_listing BEFORE INSERT ON xianshi_item "
                "BEGIN SELECT RAISE(ABORT, 'listing failed'); END"
            )

        with self.assertRaises(db_backend.IntegrityError):
            self.repository.add_xianshi_items(
                "listing-fail", "seller", 1001, "测试法器", "装备", 600000, 2, fee_charged=120000
            )

        self.assertEqual(self.scalar("SELECT COUNT(*) FROM xianshi_item"), 0)
        self.assertEqual(self.scalar("SELECT COUNT(*) FROM xianshi_listing_operations"), 0)

    def test_invalid_quantity_is_rejected(self) -> None:
        with self.assertRaises(ValueError):
            self.repository.add_xianshi_items(
                "listing-invalid", "seller", 1001, "测试法器", "装备", 600000, 0, fee_charged=0
            )

    def seed_assets(self, *, stone=500000, goods_num=5, bind_num=0, state=0) -> None:
        with db_backend.transaction(self.database) as conn:
            conn.execute("INSERT INTO user_xiuxian VALUES (%s, %s)", ("seller", stone))
            conn.execute(
                "INSERT INTO back (user_id, goods_id, goods_name, goods_type, goods_num, bind_num, state) "
                "VALUES (%s, %s, %s, %s, %s, %s, %s)",
                ("seller", 1001, "测试法器", "装备", goods_num, bind_num, state),
            )

    def test_atomic_listing_consumes_fee_and_trade_stock(self) -> None:
        self.seed_assets()
        result = self.repository.add_xianshi_items(
            "listing-assets", "seller", 1001, "测试法器", "装备", 600000, 3,
            fee_charged=180000, consume_assets=True,
        )

        self.assertTrue(result.applied)
        self.assertEqual(self.scalar("SELECT stone FROM user_xiuxian WHERE user_id=%s", ("seller",)), 320000)
        self.assertEqual(self.scalar("SELECT goods_num FROM back WHERE user_id=%s", ("seller",)), 2)
        self.assertEqual(self.scalar("SELECT COUNT(*) FROM xianshi_item"), 3)

    def test_atomic_listing_duplicate_and_conflict_do_not_consume_assets_again(self) -> None:
        self.seed_assets()
        first = self.repository.add_xianshi_items(
            "listing-idempotent", "seller", 1001, "测试法器", "装备", 600000, 2,
            fee_charged=120000, consume_assets=True,
        )
        duplicate = self.repository.add_xianshi_items(
            "listing-idempotent", "seller", 1001, "测试法器", "装备", 600000, 2,
            fee_charged=999999, consume_assets=True,
        )
        conflict = self.repository.add_xianshi_items(
            "listing-idempotent", "seller", 1001, "测试法器", "装备", 700000, 2,
            fee_charged=140000, consume_assets=True,
        )

        self.assertEqual((first.status, duplicate.status, conflict.status),
                         ("listed", "duplicate", "state_changed"))
        self.assertEqual(self.scalar("SELECT stone FROM user_xiuxian WHERE user_id=%s", ("seller",)), 380000)
        self.assertEqual(self.scalar("SELECT goods_num FROM back WHERE user_id=%s", ("seller",)), 3)
        self.assertEqual(self.scalar("SELECT COUNT(*) FROM xianshi_item"), 2)

    def test_atomic_listing_rejects_insufficient_assets_without_mutation(self) -> None:
        self.seed_assets(stone=100000, goods_num=2)
        stone_result = self.repository.add_xianshi_items(
            "listing-no-stone", "seller", 1001, "测试法器", "装备", 600000, 2,
            fee_charged=120000, consume_assets=True,
        )
        self.assertEqual(stone_result.status, "stone_insufficient")

        with db_backend.transaction(self.database) as conn:
            conn.execute("UPDATE user_xiuxian SET stone=%s", (500000,))
        stock_result = self.repository.add_xianshi_items(
            "listing-no-stock", "seller", 1001, "测试法器", "装备", 600000, 3,
            fee_charged=180000, consume_assets=True,
        )
        self.assertEqual(stock_result.status, "stock_insufficient")
        self.assertEqual(self.scalar("SELECT stone FROM user_xiuxian WHERE user_id=%s", ("seller",)), 500000)
        self.assertEqual(self.scalar("SELECT goods_num FROM back WHERE user_id=%s", ("seller",)), 2)
        self.assertEqual(self.scalar("SELECT COUNT(*) FROM xianshi_item"), 0)

    def test_listing_insert_failure_rolls_back_consumed_assets(self) -> None:
        self.seed_assets()
        with db_backend.transaction(self.database) as conn:
            conn.execute(
                "CREATE TRIGGER fail_atomic_listing BEFORE INSERT ON xianshi_item "
                "BEGIN SELECT RAISE(ABORT, 'listing failed'); END"
            )

        with self.assertRaises(db_backend.IntegrityError):
            self.repository.add_xianshi_items(
                "listing-assets-fail", "seller", 1001, "测试法器", "装备", 600000, 2,
                fee_charged=120000, consume_assets=True,
            )

        self.assertEqual(self.scalar("SELECT stone FROM user_xiuxian WHERE user_id=%s", ("seller",)), 500000)
        self.assertEqual(self.scalar("SELECT goods_num FROM back WHERE user_id=%s", ("seller",)), 5)


if __name__ == "__main__":
    unittest.main()
