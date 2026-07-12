from __future__ import annotations

import tempfile
import unittest
from pathlib import Path

import nonebot

nonebot.init()

from nonebot_plugin_xiuxian_2.xiuxian.xiuxian_trade.repository import TradeRepository
from tests.test_db_backend import db_backend


class XianshiRemovalTests(unittest.TestCase):
    def setUp(self) -> None:
        self.temp_dir = tempfile.TemporaryDirectory()
        self.database = Path(self.temp_dir.name) / "xianshi-removal.sqlite3"
        with db_backend.transaction(self.database) as conn:
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
        self.repository = TradeRepository(self.database, max_goods_num=10)
        self.repository.initialize()

    def tearDown(self) -> None:
        self.temp_dir.cleanup()

    def scalar(self, sql, params=()):
        with db_backend.connection(self.database) as conn:
            row = conn.execute(sql, params).fetchone()
            return row[0] if row else None

    def test_user_listing_is_removed_and_full_quantity_refunded_atomically(self) -> None:
        listing_id = self.repository.add_xianshi_item(
            "seller", 1001, "测试法器", "装备", 600000, 3
        )
        result = self.repository.remove_xianshi_listing("remove-1", listing_id)

        self.assertTrue(result.applied)
        self.assertEqual(result.refunded_quantity, 3)
        self.assertIsNone(self.scalar("SELECT id FROM xianshi_item WHERE id=%s", (listing_id,)))
        self.assertEqual(
            self.scalar("SELECT goods_num FROM back WHERE user_id=%s AND goods_id=%s", ("seller", 1001)), 3
        )

    def test_duplicate_operation_does_not_refund_twice(self) -> None:
        listing_id = self.repository.add_xianshi_item(
            "seller", 1001, "测试法器", "装备", 600000, 2
        )
        first = self.repository.remove_xianshi_listing("remove-repeat", listing_id)
        second = self.repository.remove_xianshi_listing("remove-repeat", listing_id)

        self.assertEqual((first.status, second.status), ("removed", "duplicate"))
        self.assertEqual(
            self.scalar("SELECT goods_num FROM back WHERE user_id=%s AND goods_id=%s", ("seller", 1001)), 2
        )

    def test_inventory_full_keeps_listing_and_inventory_unchanged(self) -> None:
        with db_backend.transaction(self.database) as conn:
            conn.execute(
                "INSERT INTO back (user_id, goods_id, goods_name, goods_type, goods_num) "
                "VALUES (%s, %s, %s, %s, %s)",
                ("seller", 1001, "测试法器", "装备", 9),
            )
        listing_id = self.repository.add_xianshi_item(
            "seller", 1001, "测试法器", "装备", 600000, 2
        )
        result = self.repository.remove_xianshi_listing("remove-full", listing_id)

        self.assertEqual(result.status, "inventory_full")
        self.assertEqual(self.scalar("SELECT quantity FROM xianshi_item WHERE id=%s", (listing_id,)), 2)
        self.assertEqual(
            self.scalar("SELECT goods_num FROM back WHERE user_id=%s AND goods_id=%s", ("seller", 1001)), 9
        )

    def test_refund_failure_rolls_back_listing_removal(self) -> None:
        listing_id = self.repository.add_xianshi_item(
            "seller", 1001, "测试法器", "装备", 600000, 1
        )
        with db_backend.transaction(self.database) as conn:
            conn.execute(
                "CREATE TRIGGER fail_refund BEFORE INSERT ON back "
                "BEGIN SELECT RAISE(ABORT, 'refund failed'); END"
            )

        with self.assertRaises(db_backend.IntegrityError):
            self.repository.remove_xianshi_listing("remove-fail", listing_id)

        self.assertEqual(self.scalar("SELECT quantity FROM xianshi_item WHERE id=%s", (listing_id,)), 1)
        self.assertEqual(self.scalar("SELECT COUNT(*) FROM xianshi_removal_operations"), 0)

    def test_system_listing_removes_without_inventory_refund(self) -> None:
        listing_id = self.repository.add_xianshi_item(
            "0", 1001, "测试法器", "装备", 600000, -1
        )
        result = self.repository.remove_xianshi_listing("remove-system", listing_id)

        self.assertTrue(result.applied)
        self.assertEqual(result.refunded_quantity, 0)
        self.assertEqual(self.scalar("SELECT COUNT(*) FROM back"), 0)


if __name__ == "__main__":
    unittest.main()
