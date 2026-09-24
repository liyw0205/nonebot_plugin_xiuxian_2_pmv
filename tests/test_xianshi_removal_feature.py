from __future__ import annotations

import tempfile
import unittest
from datetime import datetime, timezone
from pathlib import Path

from nonebot_plugin_xiuxian_2.features.trade.application import TradeApplication
from nonebot_plugin_xiuxian_2.features.trade.migrations import (
    apply_trade_xianshi_listing,
    apply_trade_xianshi_removal,
)
from nonebot_plugin_xiuxian_2.infrastructure.database import DatabaseUnitOfWork
from nonebot_plugin_xiuxian_2.plugin import build_migrations, migrations_for_database
from tests.test_db_backend import db_backend


class FixedClock:
    def now(self):
        return datetime(2026, 9, 24, 12, 0, tzinfo=timezone.utc)


class SequenceIds:
    def __init__(self):
        self.value = 0

    def new_id(self):
        self.value += 1
        return f"{self.value:032x}"


class XianshiRemovalFeatureTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.database = Path(self.temp.name) / "game.db"
        with db_backend.transaction(self.database) as conn:
            conn.execute(
                "CREATE TABLE back (user_id TEXT,goods_id INTEGER,goods_name TEXT,"
                "goods_type TEXT,goods_num INTEGER DEFAULT 0,create_time TEXT,"
                "update_time TEXT,bind_num INTEGER DEFAULT 0,state INTEGER DEFAULT 0,"
                "action_time TEXT,UNIQUE(user_id,goods_id))"
            )
            conn.execute(
                "CREATE TABLE xianshi_item (id TEXT PRIMARY KEY,user_id TEXT,goods_id INTEGER,"
                "name TEXT,type TEXT,price INTEGER,quantity INTEGER)"
            )
        with DatabaseUnitOfWork(self.database) as uow:
            apply_trade_xianshi_listing(uow)
            apply_trade_xianshi_removal(uow)
        self.ids = SequenceIds()
        self.application = TradeApplication(
            self.database,
            Path(self.temp.name) / "trade.db",
            clock=FixedClock(),
            ids=self.ids,
        )

    def tearDown(self):
        self.temp.cleanup()

    def scalar(self, query, params=()):
        with db_backend.connection(self.database) as conn:
            row = conn.execute(query, params).fetchone()
            return row[0] if row else None

    def add_listing(self, listing_id, seller="seller", goods_id=1001, name="法器", quantity=2, price=600000):
        with db_backend.transaction(self.database) as conn:
            conn.execute(
                "INSERT INTO xianshi_item VALUES(?,?,?,?,?,?,?)",
                (listing_id, seller, goods_id, name, "装备", price, quantity),
            )

    def test_single_removal_refunds_user_and_replays_without_double_refund(self):
        self.add_listing("listing-1", quantity=3)
        first = self.application.xianshi_remove_listing(
            operation_id="remove-1", listing_id="listing-1", max_goods_num=10
        )
        replay = self.application.xianshi_remove_listing(
            operation_id="remove-1", listing_id="listing-1", max_goods_num=10
        )
        conflict = self.application.xianshi_remove_listing(
            operation_id="remove-1", listing_id="different", max_goods_num=10
        )

        self.assertEqual((first.status, replay.status, conflict.status), ("removed", "duplicate", "state_changed"))
        self.assertEqual(self.scalar("SELECT COUNT(*) FROM xianshi_item"), 0)
        self.assertEqual(self.scalar("SELECT goods_num FROM back WHERE user_id='seller'"), 3)

    def test_single_removal_keeps_listing_when_inventory_is_full(self):
        with db_backend.transaction(self.database) as conn:
            conn.execute("INSERT INTO back(user_id,goods_id,goods_num) VALUES('seller',1001,9)")
        self.add_listing("listing-1", quantity=2)
        result = self.application.xianshi_remove_listing(
            operation_id="remove-full", listing_id="listing-1", max_goods_num=10
        )

        self.assertEqual(result.status, "inventory_full")
        self.assertEqual(self.scalar("SELECT quantity FROM xianshi_item WHERE id='listing-1'"), 2)
        self.assertEqual(self.scalar("SELECT goods_num FROM back WHERE user_id='seller'"), 9)

    def test_single_removal_operation_failure_rolls_back_refund_and_listing(self):
        self.add_listing("listing-1", quantity=2)
        with db_backend.transaction(self.database) as conn:
            conn.execute(
                "CREATE TRIGGER fail_xianshi_removal BEFORE INSERT ON "
                "xianshi_removal_operations BEGIN SELECT RAISE(ABORT,'operation failed'); END"
            )

        with self.assertRaises(db_backend.IntegrityError):
            self.application.xianshi_remove_listing(
                operation_id="remove-fail", listing_id="listing-1", max_goods_num=10
            )

        self.assertEqual(self.scalar("SELECT quantity FROM xianshi_item WHERE id='listing-1'"), 2)
        self.assertIsNone(self.scalar("SELECT goods_num FROM back WHERE user_id='seller'"))
        self.assertEqual(self.scalar("SELECT COUNT(*) FROM xianshi_removal_operations"), 0)

    def test_name_removal_uses_lowest_price_and_refunds_once(self):
        self.add_listing("high", quantity=3, price=800000)
        self.add_listing("low", quantity=2, price=600000)
        first = self.application.xianshi_remove_by_name(
            operation_id="remove-name", seller_id="seller", item_name="法器",
            quantity=4, max_goods_num=10,
        )
        replay = self.application.xianshi_remove_by_name(
            operation_id="remove-name", seller_id="seller", item_name="法器",
            quantity=4, max_goods_num=10,
        )

        self.assertEqual((first.status, replay.status, first.removed_quantity), ("removed", "duplicate", 4))
        self.assertIsNone(self.scalar("SELECT id FROM xianshi_item WHERE id='low'"))
        self.assertEqual(self.scalar("SELECT quantity FROM xianshi_item WHERE id='high'"), 1)
        self.assertEqual(self.scalar("SELECT goods_num FROM back WHERE user_id='seller'"), 4)

    def test_clear_preflights_all_refunds_and_rolls_back_when_one_is_full(self):
        self.add_listing("seller-row", quantity=2)
        self.add_listing("other-row", seller="other", goods_id=1002, name="丹药", quantity=2)
        with db_backend.transaction(self.database) as conn:
            conn.execute("INSERT INTO back(user_id,goods_id,goods_num) VALUES('other',1002,9)")

        result = self.application.xianshi_clear_all(
            operation_id="clear-full", max_goods_num=10
        )

        self.assertEqual(result.status, "inventory_full")
        self.assertEqual(self.scalar("SELECT COUNT(*) FROM xianshi_item"), 2)
        self.assertIsNone(self.scalar("SELECT goods_num FROM back WHERE user_id='seller'"))
        self.assertEqual(self.scalar("SELECT goods_num FROM back WHERE user_id='other'"), 9)
        self.assertEqual(self.scalar("SELECT COUNT(*) FROM xianshi_clear_operations"), 0)

    def test_clear_refunds_users_but_not_system_and_replays(self):
        self.add_listing("seller-row", quantity=2)
        self.add_listing("system-row", seller="0", goods_id=1002, name="丹药", quantity=-1)
        first = self.application.xianshi_clear_all(operation_id="clear-1", max_goods_num=10)
        replay = self.application.xianshi_clear_all(operation_id="clear-1", max_goods_num=10)

        self.assertEqual((first.status, replay.status), ("cleared", "duplicate"))
        self.assertEqual((first.listing_count, first.refunded_quantity), (2, 2))
        self.assertEqual(self.scalar("SELECT COUNT(*) FROM xianshi_item"), 0)
        self.assertEqual(self.scalar("SELECT goods_num FROM back WHERE user_id='seller'"), 2)

    def test_clear_operation_failure_rolls_back_all_refunds(self):
        self.add_listing("seller-row", quantity=2)
        with db_backend.transaction(self.database) as conn:
            conn.execute(
                "CREATE TRIGGER fail_xianshi_clear BEFORE INSERT ON "
                "xianshi_clear_operations BEGIN SELECT RAISE(ABORT,'operation failed'); END"
            )

        with self.assertRaises(db_backend.IntegrityError):
            self.application.xianshi_clear_all(operation_id="clear-fail", max_goods_num=10)

        self.assertEqual(self.scalar("SELECT COUNT(*) FROM xianshi_item"), 1)
        self.assertIsNone(self.scalar("SELECT goods_num FROM back WHERE user_id='seller'"))
        self.assertEqual(self.scalar("SELECT COUNT(*) FROM xianshi_clear_operations"), 0)


class XianshiRemovalMigrationRoutingTests(unittest.TestCase):
    def test_removal_operation_migration_is_game_database_only(self):
        migrations = build_migrations()
        game_versions = {item.version for item in migrations_for_database(migrations, "game_db")}
        trade_versions = {item.version for item in migrations_for_database(migrations, "trade_db")}
        self.assertIn("trade.012", game_versions)
        self.assertNotIn("trade.012", trade_versions)


if __name__ == "__main__":
    unittest.main()
