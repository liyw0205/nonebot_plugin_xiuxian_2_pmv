from __future__ import annotations

import tempfile
import unittest
from datetime import datetime, timezone
from pathlib import Path

from nonebot_plugin_xiuxian_2.features.trade.application import TradeApplication
from nonebot_plugin_xiuxian_2.features.trade.migrations import (
    apply_trade_xianshi_plan_listing,
    apply_trade_xianshi_listing,
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


class XianshiPlanListingFeatureTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.database = Path(self.temp.name) / "game.db"
        with db_backend.transaction(self.database) as conn:
            conn.execute(
                "CREATE TABLE user_xiuxian (user_id TEXT PRIMARY KEY,stone INTEGER NOT NULL,"
                "user_stamina INTEGER NOT NULL)"
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
            conn.execute("INSERT INTO user_xiuxian VALUES ('seller',500000,100)")
            conn.execute(
                "INSERT INTO back(user_id,goods_id,goods_num) VALUES('seller',1001,3)"
            )
            conn.execute(
                "INSERT INTO back(user_id,goods_id,goods_num) VALUES('seller',1002,2)"
            )
        with DatabaseUnitOfWork(self.database) as uow:
            apply_trade_xianshi_listing(uow)
            apply_trade_xianshi_plan_listing(uow)
        self.plan = [
            {
                "goods_id": 1001,
                "name": "测试法器",
                "goods_type": "装备",
                "price": 600000,
                "quantity": 2,
            },
            {
                "goods_id": 1002,
                "name": "灵草",
                "goods_type": "药材",
                "price": 800000,
                "quantity": 1,
            },
        ]
        self.application = TradeApplication(
            self.database,
            Path(self.temp.name) / "trade.db",
            clock=FixedClock(),
            ids=SequenceIds(),
        )

    def tearDown(self):
        self.temp.cleanup()

    def list_plan(self, operation_id="auto-list-1", *, plan=None, stamina_cost=30):
        return self.application.xianshi_list_plan(
            operation_id=operation_id,
            seller_id="seller",
            listing_plan=self.plan if plan is None else plan,
            stamina_cost=stamina_cost,
        )

    def scalar(self, query, params=()):
        with db_backend.connection(self.database) as conn:
            return conn.execute(query, params).fetchone()[0]

    def test_plan_listing_charges_fee_stamina_and_all_stock_atomically(self):
        result = self.list_plan()

        self.assertEqual(
            (result.status, result.listed_quantity, result.fee_charged),
            ("listed", 3, 200000),
        )
        self.assertEqual(result.stamina_charged, 30)
        self.assertEqual(self.scalar("SELECT stone FROM user_xiuxian"), 300000)
        self.assertEqual(self.scalar("SELECT user_stamina FROM user_xiuxian"), 70)
        self.assertEqual(
            self.scalar("SELECT goods_num FROM back WHERE goods_id=1001"), 1
        )
        self.assertEqual(
            self.scalar("SELECT goods_num FROM back WHERE goods_id=1002"), 1
        )
        self.assertEqual(self.scalar("SELECT COUNT(*) FROM xianshi_item"), 3)
        self.assertEqual(
            self.scalar("SELECT COUNT(*) FROM xianshi_plan_listing_operations"), 1
        )

    def test_duplicate_replays_and_changed_plan_conflicts_without_recharging(self):
        first = self.list_plan()
        replay = self.list_plan()
        changed_plan = [dict(entry) for entry in self.plan]
        changed_plan[0]["price"] += 1
        conflict = self.list_plan(plan=changed_plan)

        self.assertEqual(
            (first.status, replay.status, conflict.status),
            ("listed", "duplicate", "state_changed"),
        )
        self.assertEqual((replay.fee_charged, replay.stamina_charged), (200000, 30))
        self.assertEqual(self.scalar("SELECT stone FROM user_xiuxian"), 300000)
        self.assertEqual(self.scalar("SELECT user_stamina FROM user_xiuxian"), 70)
        self.assertEqual(self.scalar("SELECT COUNT(*) FROM xianshi_item"), 3)

    def test_stamina_shortfall_leaves_assets_unchanged(self):
        with db_backend.transaction(self.database) as conn:
            conn.execute("UPDATE user_xiuxian SET user_stamina=29")

        result = self.list_plan()

        self.assertEqual(result.status, "stamina_insufficient")
        self.assertEqual(self.scalar("SELECT stone FROM user_xiuxian"), 500000)
        self.assertEqual(self.scalar("SELECT user_stamina FROM user_xiuxian"), 29)
        self.assertEqual(self.scalar("SELECT goods_num FROM back WHERE goods_id=1001"), 3)
        self.assertEqual(self.scalar("SELECT COUNT(*) FROM xianshi_item"), 0)

    def test_stone_shortfall_does_not_consume_stamina_or_stock(self):
        with db_backend.transaction(self.database) as conn:
            conn.execute("UPDATE user_xiuxian SET stone=199999")

        result = self.list_plan()

        self.assertEqual(result.status, "stone_insufficient")
        self.assertEqual(self.scalar("SELECT stone FROM user_xiuxian"), 199999)
        self.assertEqual(self.scalar("SELECT user_stamina FROM user_xiuxian"), 100)
        self.assertEqual(self.scalar("SELECT goods_num FROM back WHERE goods_id=1001"), 3)
        self.assertEqual(self.scalar("SELECT COUNT(*) FROM xianshi_item"), 0)

    def test_later_item_stock_shortfall_leaves_entire_plan_unchanged(self):
        with db_backend.transaction(self.database) as conn:
            conn.execute("UPDATE back SET goods_num=0 WHERE goods_id=1002")

        result = self.list_plan()

        self.assertEqual(result.status, "stock_insufficient")
        self.assertEqual(self.scalar("SELECT stone FROM user_xiuxian"), 500000)
        self.assertEqual(self.scalar("SELECT user_stamina FROM user_xiuxian"), 100)
        self.assertEqual(self.scalar("SELECT goods_num FROM back WHERE goods_id=1001"), 3)
        self.assertEqual(self.scalar("SELECT COUNT(*) FROM xianshi_item"), 0)

    def test_operation_insert_failure_rolls_back_every_asset_change(self):
        with db_backend.transaction(self.database) as conn:
            conn.execute(
                "CREATE TRIGGER fail_plan_operation "
                "BEFORE INSERT ON xianshi_plan_listing_operations "
                "BEGIN SELECT RAISE(ABORT,'operation failed'); END"
            )

        with self.assertRaises(db_backend.IntegrityError):
            self.list_plan()

        self.assertEqual(self.scalar("SELECT stone FROM user_xiuxian"), 500000)
        self.assertEqual(self.scalar("SELECT user_stamina FROM user_xiuxian"), 100)
        self.assertEqual(self.scalar("SELECT goods_num FROM back WHERE goods_id=1001"), 3)
        self.assertEqual(self.scalar("SELECT goods_num FROM back WHERE goods_id=1002"), 2)
        self.assertEqual(self.scalar("SELECT COUNT(*) FROM xianshi_item"), 0)
        self.assertEqual(
            self.scalar("SELECT COUNT(*) FROM xianshi_plan_listing_operations"), 0
        )

    def test_migration_upgrades_historical_operation_table(self):
        other = Path(self.temp.name) / "old-plan.db"
        with db_backend.transaction(other) as conn:
            conn.execute(
                "CREATE TABLE xianshi_plan_listing_operations ("
                "operation_id TEXT PRIMARY KEY,seller_id TEXT NOT NULL,"
                "listing_plan TEXT NOT NULL,listed_quantity INTEGER NOT NULL,"
                "fee_charged INTEGER NOT NULL)"
            )
        with DatabaseUnitOfWork(other) as uow:
            apply_trade_xianshi_plan_listing(uow)
        with db_backend.connection(other) as conn:
            columns = {
                row[1]
                for row in conn.execute(
                    "PRAGMA table_info(xianshi_plan_listing_operations)"
                )
            }
        self.assertIn("stamina_cost", columns)


class XianshiPlanListingMigrationRoutingTests(unittest.TestCase):
    def test_plan_listing_migration_is_game_database_only(self):
        migrations = build_migrations()
        game_versions = {
            item.version for item in migrations_for_database(migrations, "game_db")
        }
        trade_versions = {
            item.version for item in migrations_for_database(migrations, "trade_db")
        }

        self.assertIn("trade.011", game_versions)
        self.assertNotIn("trade.011", trade_versions)


if __name__ == "__main__":
    unittest.main()
