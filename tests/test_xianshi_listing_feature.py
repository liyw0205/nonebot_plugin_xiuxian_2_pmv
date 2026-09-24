from __future__ import annotations

import tempfile
import unittest
from datetime import datetime, timezone
from pathlib import Path

from nonebot_plugin_xiuxian_2.features.trade.application import TradeApplication
from nonebot_plugin_xiuxian_2.features.trade.migrations import apply_trade_xianshi_listing
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


class XianshiListingFeatureTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.database = Path(self.temp.name) / "game.db"
        with db_backend.transaction(self.database) as conn:
            conn.execute(
                "CREATE TABLE user_xiuxian (user_id TEXT PRIMARY KEY,stone INTEGER NOT NULL,"
                "user_stamina INTEGER NOT NULL DEFAULT 100)"
            )
            conn.execute(
                "CREATE TABLE back (user_id TEXT,goods_id INTEGER,goods_num INTEGER,"
                "bind_num INTEGER DEFAULT 0,state INTEGER DEFAULT 0,update_time TEXT,"
                "action_time TEXT,UNIQUE(user_id,goods_id))"
            )
            conn.execute(
                "CREATE TABLE xianshi_item (id TEXT PRIMARY KEY,user_id TEXT,goods_id INTEGER,"
                "name TEXT,type TEXT,price INTEGER,quantity INTEGER)"
            )
            conn.execute("INSERT INTO user_xiuxian VALUES ('seller',1000000,100)")
            conn.execute("INSERT INTO back(user_id,goods_id,goods_num) VALUES('seller',1001,5)")
        with DatabaseUnitOfWork(self.database) as uow:
            apply_trade_xianshi_listing(uow)
        self.ids = SequenceIds()
        self.application = TradeApplication(
            self.database,
            Path(self.temp.name) / "trade.db",
            clock=FixedClock(),
            ids=self.ids,
        )

    def tearDown(self):
        self.temp.cleanup()

    def list_items(
        self, operation_id="list-1", *, price=600000, quantity=2, stamina_cost=0
    ):
        return self.application.xianshi_list_items(
            operation_id=operation_id,
            seller_id="seller",
            goods_id=1001,
            name="法器",
            goods_type="装备",
            price=price,
            quantity=quantity,
            stamina_cost=stamina_cost,
        )

    def list_system_item(self, operation_id, *, price=600000, quantity=-1):
        return self.application.xianshi_list_system_item(
            operation_id=operation_id,
            goods_id=1001,
            name="法器",
            goods_type="装备",
            price=price,
            quantity=quantity,
        )

    def scalar(self, query):
        with db_backend.connection(self.database) as conn:
            return conn.execute(query).fetchone()[0]

    def test_listing_consumes_fee_and_unbound_stock_atomically(self):
        result = self.list_items()
        self.assertEqual((result.status, result.listed_quantity, result.fee_charged), ("listed", 2, 120000))
        self.assertEqual(self.scalar("SELECT stone FROM user_xiuxian WHERE user_id='seller'"), 880000)
        self.assertEqual(self.scalar("SELECT goods_num FROM back WHERE user_id='seller'"), 3)
        self.assertEqual(self.scalar("SELECT COUNT(*) FROM xianshi_item"), 2)
        self.assertEqual(self.scalar("SELECT COUNT(*) FROM xianshi_listing_operations"), 1)

    def test_duplicate_replays_without_charging_or_creating_more_rows(self):
        first = self.list_items()
        replay = self.list_items()
        conflict = self.list_items(price=700000)
        self.assertEqual((first.status, replay.status, conflict.status), ("listed", "duplicate", "state_changed"))
        self.assertEqual(self.scalar("SELECT stone FROM user_xiuxian WHERE user_id='seller'"), 880000)
        self.assertEqual(self.scalar("SELECT COUNT(*) FROM xianshi_item"), 2)

    def test_rejections_leave_both_assets_unchanged(self):
        with db_backend.transaction(self.database) as conn:
            conn.execute("UPDATE user_xiuxian SET stone=100 WHERE user_id='seller'")
        result = self.list_items()
        self.assertEqual(result.status, "stone_insufficient")
        self.assertEqual(self.scalar("SELECT goods_num FROM back WHERE user_id='seller'"), 5)
        self.assertEqual(self.scalar("SELECT COUNT(*) FROM xianshi_item"), 0)

        with db_backend.transaction(self.database) as conn:
            conn.execute("UPDATE user_xiuxian SET stone=1000000 WHERE user_id='seller'")
            conn.execute("UPDATE back SET goods_num=1 WHERE user_id='seller'")
        result = self.list_items(operation_id="list-2")
        self.assertEqual(result.status, "stock_insufficient")
        self.assertEqual(self.scalar("SELECT stone FROM user_xiuxian WHERE user_id='seller'"), 1000000)

    def test_listing_insert_failure_rolls_back_assets_and_operation(self):
        with db_backend.transaction(self.database) as conn:
            conn.execute(
                "CREATE TRIGGER fail_xianshi_listing BEFORE INSERT ON xianshi_item "
                "BEGIN SELECT RAISE(ABORT,'listing failed'); END"
            )
        with self.assertRaises(db_backend.IntegrityError):
            self.list_items()
        self.assertEqual(self.scalar("SELECT stone FROM user_xiuxian WHERE user_id='seller'"), 1000000)
        self.assertEqual(self.scalar("SELECT goods_num FROM back WHERE user_id='seller'"), 5)
        self.assertEqual(self.scalar("SELECT COUNT(*) FROM xianshi_listing_operations"), 0)

    def test_fast_listing_charges_stamina_and_replays_without_double_charge(self):
        first = self.list_items("fast-list-1", stamina_cost=10)
        replay = self.list_items("fast-list-1", stamina_cost=10)
        conflict = self.list_items("fast-list-1", stamina_cost=0)

        self.assertEqual(
            (first.status, replay.status, conflict.status),
            ("listed", "duplicate", "state_changed"),
        )
        self.assertEqual((first.stamina_charged, replay.stamina_charged), (10, 10))
        self.assertEqual(self.scalar("SELECT user_stamina FROM user_xiuxian"), 90)
        self.assertEqual(self.scalar("SELECT stone FROM user_xiuxian"), 880000)
        self.assertEqual(self.scalar("SELECT goods_num FROM back"), 3)
        self.assertEqual(self.scalar("SELECT COUNT(*) FROM xianshi_item"), 2)

    def test_fast_stamina_shortfall_leaves_all_assets_unchanged(self):
        with db_backend.transaction(self.database) as conn:
            conn.execute("UPDATE user_xiuxian SET user_stamina=9")

        result = self.list_items("fast-list-short", stamina_cost=10)

        self.assertEqual(result.status, "stamina_insufficient")
        self.assertEqual(self.scalar("SELECT user_stamina FROM user_xiuxian"), 9)
        self.assertEqual(self.scalar("SELECT stone FROM user_xiuxian"), 1000000)
        self.assertEqual(self.scalar("SELECT goods_num FROM back"), 5)
        self.assertEqual(self.scalar("SELECT COUNT(*) FROM xianshi_item"), 0)

    def test_fast_operation_failure_rolls_back_stamina_and_other_assets(self):
        with db_backend.transaction(self.database) as conn:
            conn.execute(
                "CREATE TRIGGER fail_fast_operation "
                "BEFORE INSERT ON xianshi_listing_operations "
                "BEGIN SELECT RAISE(ABORT,'operation failed'); END"
            )

        with self.assertRaises(db_backend.IntegrityError):
            self.list_items("fast-list-fail", stamina_cost=10)

        self.assertEqual(self.scalar("SELECT user_stamina FROM user_xiuxian"), 100)
        self.assertEqual(self.scalar("SELECT stone FROM user_xiuxian"), 1000000)
        self.assertEqual(self.scalar("SELECT goods_num FROM back"), 5)
        self.assertEqual(self.scalar("SELECT COUNT(*) FROM xianshi_item"), 0)
        self.assertEqual(self.scalar("SELECT COUNT(*) FROM xianshi_listing_operations"), 0)

    def test_fast_stone_shortfall_does_not_consume_stamina_or_stock(self):
        with db_backend.transaction(self.database) as conn:
            conn.execute("UPDATE user_xiuxian SET stone=119999")

        result = self.list_items("fast-list-no-stone", stamina_cost=10)

        self.assertEqual(result.status, "stone_insufficient")
        self.assertEqual(self.scalar("SELECT user_stamina FROM user_xiuxian"), 100)
        self.assertEqual(self.scalar("SELECT stone FROM user_xiuxian"), 119999)
        self.assertEqual(self.scalar("SELECT goods_num FROM back"), 5)
        self.assertEqual(self.scalar("SELECT COUNT(*) FROM xianshi_item"), 0)

    def test_system_listing_preserves_unlimited_and_fixed_quantities_with_replay(self):
        unlimited = self.list_system_item("system-unlimited")
        replay = self.list_system_item("system-unlimited")
        conflict = self.list_system_item("system-unlimited", price=700000)
        fixed = self.list_system_item("system-fixed", quantity=8)

        self.assertEqual(
            (unlimited.status, replay.status, conflict.status, fixed.status),
            ("listed", "duplicate", "state_changed", "listed"),
        )
        with db_backend.connection(self.database) as conn:
            rows = conn.execute(
                "SELECT user_id,quantity,price FROM xianshi_item"
            ).fetchall()
        self.assertEqual(
            {tuple(row) for row in rows},
            {("0", -1, 600000), ("0", 8, 600000)},
        )
        self.assertEqual(self.scalar("SELECT COUNT(*) FROM xianshi_listing_operations"), 2)
        self.assertEqual(self.scalar("SELECT stone FROM user_xiuxian"), 1000000)
        self.assertEqual(self.scalar("SELECT goods_num FROM back"), 5)

    def test_system_operation_failure_rolls_back_listing(self):
        with db_backend.transaction(self.database) as conn:
            conn.execute(
                "CREATE TRIGGER fail_system_operation "
                "BEFORE INSERT ON xianshi_listing_operations "
                "BEGIN SELECT RAISE(ABORT,'operation failed'); END"
            )

        with self.assertRaises(db_backend.IntegrityError):
            self.list_system_item("system-fail")

        self.assertEqual(self.scalar("SELECT COUNT(*) FROM xianshi_item"), 0)
        self.assertEqual(self.scalar("SELECT COUNT(*) FROM xianshi_listing_operations"), 0)

    def test_migration_upgrades_historical_operation_table(self):
        other = Path(self.temp.name) / "old.db"
        with db_backend.transaction(other) as conn:
            conn.execute(
                "CREATE TABLE xianshi_listing_operations (operation_id TEXT PRIMARY KEY,"
                "seller_id TEXT NOT NULL,goods_id INTEGER NOT NULL,name TEXT NOT NULL,"
                "goods_type TEXT NOT NULL,price INTEGER NOT NULL,requested_quantity INTEGER NOT NULL,"
                "listed_quantity INTEGER NOT NULL,fee_charged INTEGER NOT NULL)"
            )
        with DatabaseUnitOfWork(other) as uow:
            apply_trade_xianshi_listing(uow)
        with db_backend.connection(other) as conn:
            columns = {row[1] for row in conn.execute("PRAGMA table_info(xianshi_listing_operations)")}
        self.assertIn("stamina_cost", columns)


class XianshiListingMigrationRoutingTests(unittest.TestCase):
    def test_listing_operation_migration_is_game_database_only(self):
        migrations = build_migrations()
        game_versions = {
            item.version for item in migrations_for_database(migrations, "game_db")
        }
        trade_versions = {
            item.version for item in migrations_for_database(migrations, "trade_db")
        }

        self.assertIn("trade.010", game_versions)
        self.assertNotIn("trade.010", trade_versions)


if __name__ == "__main__":
    unittest.main()
