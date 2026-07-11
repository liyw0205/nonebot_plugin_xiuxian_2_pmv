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


if __name__ == "__main__":
    unittest.main()
