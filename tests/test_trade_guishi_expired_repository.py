from __future__ import annotations

import sqlite3
import tempfile
import unittest
from datetime import datetime, timezone
from pathlib import Path

from nonebot_plugin_xiuxian_2.features.trade.application import TradeApplication
from nonebot_plugin_xiuxian_2.features.trade.guishi_expired_repository import (
    GuishiExpiredOrderSqlRepository,
)
from nonebot_plugin_xiuxian_2.features.trade.migrations import (
    apply_trade_guishi_expired_cleanup,
)
from nonebot_plugin_xiuxian_2.infrastructure.database import DatabaseUnitOfWork
from nonebot_plugin_xiuxian_2.plugin import build_migrations, migrations_for_database


class FixedClock:
    def now(self) -> datetime:
        return datetime(2026, 9, 24, 12, 34, 56, tzinfo=timezone.utc)


class GuishiExpiredRepositoryTests(unittest.TestCase):
    def setUp(self) -> None:
        self.temp_dir = tempfile.TemporaryDirectory()
        root = Path(self.temp_dir.name)
        self.game_database = root / "game.db"
        self.trade_database = root / "trade.db"
        with DatabaseUnitOfWork(self.game_database) as uow:
            uow.execute(
                "CREATE TABLE back(user_id TEXT,goods_id INTEGER,goods_name TEXT,"
                "goods_type TEXT,goods_num INTEGER,create_time TEXT,update_time TEXT,"
                "bind_num INTEGER DEFAULT 0,UNIQUE(user_id,goods_id))"
            )
        with DatabaseUnitOfWork(self.trade_database) as uow:
            uow.execute(
                "CREATE TABLE guishi_item(id TEXT PRIMARY KEY,user_id TEXT,item_id INTEGER,"
                "item_name TEXT,item_type TEXT,price INTEGER,quantity INTEGER,"
                "filled_quantity INTEGER DEFAULT 0)"
            )
            uow.execute(
                "INSERT INTO guishi_item VALUES('b1','seller',1001,'灵草','baitan',20,10,4)"
            )
            apply_trade_guishi_expired_cleanup(uow)
        self.repository = GuishiExpiredOrderSqlRepository(
            self.game_database, self.trade_database, clock=FixedClock()
        )

    def tearDown(self) -> None:
        self.temp_dir.cleanup()

    def inventory(self):
        with DatabaseUnitOfWork(self.game_database) as uow:
            row = uow.query_one(
                "SELECT goods_num,goods_type,update_time FROM back "
                "WHERE user_id='seller' AND goods_id=1001"
            )
            return None if row is None else (int(row["goods_num"]), row["goods_type"], row["update_time"])

    def order_exists(self, order_id: str = "b1") -> bool:
        with DatabaseUnitOfWork(self.trade_database) as uow:
            return uow.query_one("SELECT 1 AS present FROM guishi_item WHERE id=?", (order_id,)) is not None

    def test_clear_refunds_unsold_inventory_and_replays(self) -> None:
        first = self.repository.clear_baitan(
            operation_id="expire-b1",
            order_id="b1",
            goods_type="药材",
            max_goods_num=10,
            expected_user_id="seller",
        )
        duplicate = self.repository.clear_baitan(
            operation_id="expire-b1",
            order_id="b1",
            goods_type="药材",
            max_goods_num=10,
            expected_user_id="seller",
        )
        conflict = self.repository.clear_baitan(
            operation_id="expire-b1",
            order_id="other",
            goods_type="药材",
            max_goods_num=10,
            expected_user_id="seller",
        )
        self.assertEqual((first.status, first.refunded_quantity), ("cleared", 6))
        self.assertEqual((duplicate.status, duplicate.refunded_quantity), ("duplicate", 6))
        self.assertEqual(conflict.status, "operation_conflict")
        self.assertEqual(self.inventory()[:2], (6, "药材"))
        self.assertFalse(self.order_exists())

    def test_full_inventory_and_owner_or_type_rejection_keep_order(self) -> None:
        with DatabaseUnitOfWork(self.game_database) as uow:
            uow.execute(
                "INSERT INTO back VALUES('seller',1001,'灵草','药材',5,NULL,NULL,0)"
            )
        full = self.repository.clear_baitan(
            operation_id="full-b1", order_id="b1", goods_type="药材", max_goods_num=10
        )
        self.assertEqual(full.status, "inventory_full")
        self.assertTrue(self.order_exists())

        owner = self.repository.clear_baitan(
            operation_id="owner-b1",
            order_id="b1",
            goods_type="药材",
            max_goods_num=10,
            expected_user_id="other",
        )
        self.assertEqual(owner.status, "not_owner")
        with DatabaseUnitOfWork(self.trade_database) as uow:
            uow.execute("UPDATE guishi_item SET item_type='qiugou' WHERE id='b1'")
        invalid = self.repository.clear_baitan(
            operation_id="invalid-b1", order_id="b1", goods_type="药材", max_goods_num=10
        )
        self.assertEqual(invalid.status, "not_baitan")

    def test_delete_failure_rolls_back_cross_database_refund(self) -> None:
        with DatabaseUnitOfWork(self.trade_database) as uow:
            uow.execute(
                "CREATE TRIGGER reject_expired_delete BEFORE DELETE ON guishi_item "
                "BEGIN SELECT RAISE(ABORT,'reject'); END"
            )
        with self.assertRaises(sqlite3.IntegrityError):
            self.repository.clear_baitan(
                operation_id="rollback-b1",
                order_id="b1",
                goods_type="药材",
                max_goods_num=10,
            )
        self.assertIsNone(self.inventory())
        self.assertTrue(self.order_exists())

    def test_application_and_trade_only_migration(self) -> None:
        application = TradeApplication(self.game_database, self.trade_database, clock=FixedClock())
        result = application.guishi_clear_expired_baitan(
            operation_id="app-b1",
            order_id="b1",
            goods_type="药材",
            max_goods_num=10,
        )
        self.assertTrue(result.cleared)
        migrations = build_migrations()
        game = {item.version for item in migrations_for_database(migrations, "game_db")}
        trade = {item.version for item in migrations_for_database(migrations, "trade_db")}
        self.assertIn("trade.008", trade)
        self.assertNotIn("trade.008", game)


if __name__ == "__main__":
    unittest.main()
