from __future__ import annotations

import tempfile
import unittest
from pathlib import Path

import nonebot

nonebot.init()

from nonebot_plugin_xiuxian_2.xiuxian.xiuxian_trade.repository import TradeRepository
from tests.test_db_backend import db_backend


class XianshiPlanListingBatchTests(unittest.TestCase):
    def setUp(self) -> None:
        self.temp_dir = tempfile.TemporaryDirectory()
        self.database = Path(self.temp_dir.name) / "xianshi-plan.sqlite3"
        with db_backend.transaction(self.database) as conn:
            TradeRepository.ensure_schema(conn)
        self.repository = TradeRepository(self.database, max_goods_num=99)
        self.repository.initialize()
        self.plan = [
            {"goods_id": 1001, "name": "测试法器", "goods_type": "装备", "price": 600000, "quantity": 2},
            {"goods_id": 1002, "name": "灵草", "goods_type": "药材", "price": 800000, "quantity": 1},
        ]

    def tearDown(self) -> None:
        self.temp_dir.cleanup()

    def scalar(self, sql, params=()):
        with db_backend.connection(self.database) as conn:
            row = conn.execute(sql, params).fetchone()
            return row[0] if row else None

    def test_plan_listing_creates_all_rows_and_records_actual_fee(self) -> None:
        result = self.repository.add_xianshi_plan_items(
            "plan-1", "seller", self.plan, fee_charged=200000
        )

        self.assertEqual(result.status, "listed")
        self.assertEqual((result.listed_quantity, result.fee_charged, result.fee_refund), (3, 200000, 0))
        self.assertEqual(self.scalar("SELECT COUNT(*) FROM xianshi_item"), 3)
        self.assertEqual(self.scalar("SELECT COUNT(*) FROM xianshi_plan_listing_operations"), 1)

    def test_duplicate_reuses_first_result_without_creating_more_rows(self) -> None:
        first = self.repository.add_xianshi_plan_items(
            "plan-repeat", "seller", self.plan, fee_charged=200000
        )
        second = self.repository.add_xianshi_plan_items(
            "plan-repeat", "seller", self.plan, fee_charged=500000
        )

        self.assertEqual((first.status, second.status), ("listed", "duplicate"))
        self.assertEqual((second.listed_quantity, second.fee_charged, second.fee_refund), (3, 200000, 0))
        self.assertEqual(self.scalar("SELECT COUNT(*) FROM xianshi_item"), 3)

    def test_insert_failure_rolls_back_all_rows_and_operation(self) -> None:
        with db_backend.transaction(self.database) as conn:
            conn.execute(
                "CREATE TRIGGER fail_plan_insert BEFORE INSERT ON xianshi_item "
                "BEGIN SELECT RAISE(ABORT, 'plan failed'); END"
            )

        with self.assertRaises(db_backend.IntegrityError):
            self.repository.add_xianshi_plan_items(
                "plan-fail", "seller", self.plan, fee_charged=200000
            )

        self.assertEqual(self.scalar("SELECT COUNT(*) FROM xianshi_item"), 0)
        self.assertEqual(self.scalar("SELECT COUNT(*) FROM xianshi_plan_listing_operations"), 0)


if __name__ == "__main__":
    unittest.main()
