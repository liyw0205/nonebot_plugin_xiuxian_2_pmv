from __future__ import annotations

import sqlite3
import tempfile
import unittest
from datetime import datetime, timezone
from pathlib import Path

from nonebot_plugin_xiuxian_2.features.trade.guishi_cancel_repository import (
    GuishiOrderCancelSqlRepository,
)
from nonebot_plugin_xiuxian_2.features.trade.migrations import (
    apply_trade_guishi_order_cancel,
)
from nonebot_plugin_xiuxian_2.infrastructure.database import DatabaseUnitOfWork
from nonebot_plugin_xiuxian_2.plugin import build_migrations, migrations_for_database


class FixedClock:
    def now(self) -> datetime:
        return datetime(2026, 9, 24, 12, 34, 56, tzinfo=timezone.utc)


class GuishiCancelRepositoryTests(unittest.TestCase):
    def setUp(self) -> None:
        self.temp_dir = tempfile.TemporaryDirectory()
        root = Path(self.temp_dir.name)
        self.game_database = root / "game.db"
        self.trade_database = root / "trade.db"
        with DatabaseUnitOfWork(self.game_database) as uow:
            uow.execute(
                "CREATE TABLE back("
                "user_id TEXT,goods_id INTEGER,goods_name TEXT,goods_type TEXT,"
                "goods_num INTEGER DEFAULT 0,create_time TEXT,update_time TEXT,"
                "bind_num INTEGER DEFAULT 0,state INTEGER DEFAULT 0,"
                "UNIQUE(user_id,goods_id))"
            )
            uow.execute(
                "INSERT INTO back(user_id,goods_id,goods_name,goods_type,goods_num,bind_num,state) "
                "VALUES(?,?,?,?,?,?,?)",
                ("seller", 1001, "灵草", "药材", 4, 0, 0),
            )
        with DatabaseUnitOfWork(self.trade_database) as uow:
            uow.execute(
                "CREATE TABLE guishi_info("
                "user_id TEXT PRIMARY KEY,stored_stone INTEGER,items TEXT)"
            )
            uow.execute(
                "CREATE TABLE guishi_item("
                "id TEXT PRIMARY KEY,user_id TEXT,item_id INTEGER,item_name TEXT,item_type TEXT,"
                "price INTEGER,quantity INTEGER,filled_quantity INTEGER DEFAULT 0)"
            )
            uow.execute(
                "INSERT INTO guishi_info(user_id,stored_stone,items) VALUES(?,?,?)",
                ("buyer", 10, "{}"),
            )
            uow.execute(
                "INSERT INTO guishi_item(id,user_id,item_id,item_name,item_type,price,quantity,filled_quantity) "
                "VALUES(?,?,?,?,?,?,?,?)",
                ("q1", "buyer", 2001, "灵石草", "qiugou", 10, 5, 2),
            )
            uow.execute(
                "INSERT INTO guishi_item(id,user_id,item_id,item_name,item_type,price,quantity,filled_quantity) "
                "VALUES(?,?,?,?,?,?,?,?)",
                ("b1", "seller", 1001, "灵草", "baitan", 20, 6, 2),
            )
            apply_trade_guishi_order_cancel(uow)
        self.repository = GuishiOrderCancelSqlRepository(
            self.game_database,
            self.trade_database,
            clock=FixedClock(),
        )

    def tearDown(self) -> None:
        self.temp_dir.cleanup()

    def test_qiugou_refunds_unfilled_stone_and_replays(self) -> None:
        first = self.repository.cancel_qiugou(
            operation_id="cancel-q1",
            user_id="buyer",
            order_id="q1",
        )
        duplicate = self.repository.cancel_qiugou(
            operation_id="cancel-q1",
            user_id="buyer",
            order_id="q1",
        )
        conflict = self.repository.cancel_qiugou(
            operation_id="cancel-q1",
            user_id="buyer",
            order_id="missing",
        )

        self.assertEqual((first.status, first.refunded_stone), ("cancelled", 30))
        self.assertEqual((duplicate.status, duplicate.refunded_stone), ("duplicate", 30))
        self.assertEqual(conflict.status, "operation_conflict")
        with DatabaseUnitOfWork(self.trade_database) as uow:
            balance = uow.query_one(
                "SELECT stored_stone FROM guishi_info WHERE user_id='buyer'"
            )
            order = uow.query_one("SELECT 1 AS present FROM guishi_item WHERE id='q1'")
            operations = uow.query_one(
                "SELECT COUNT(*) AS count FROM guishi_order_cancel_operations"
            )
        self.assertEqual(int(balance["stored_stone"]), 40)
        self.assertIsNone(order)
        self.assertEqual(int(operations["count"]), 1)

    def test_qiugou_owner_and_delete_failure_do_not_refund(self) -> None:
        owner = self.repository.cancel_qiugou(
            operation_id="wrong-owner",
            user_id="other",
            order_id="q1",
        )
        self.assertEqual(owner.status, "not_owner")
        with DatabaseUnitOfWork(self.trade_database) as uow:
            uow.execute(
                "CREATE TRIGGER reject_qiugou_cancel BEFORE DELETE ON guishi_item "
                "WHEN OLD.id='q1' BEGIN SELECT RAISE(ABORT,'reject'); END"
            )
        with self.assertRaises(sqlite3.IntegrityError):
            self.repository.cancel_qiugou(
                operation_id="rollback-q1",
                user_id="buyer",
                order_id="q1",
            )
        with DatabaseUnitOfWork(self.trade_database) as uow:
            balance = uow.query_one(
                "SELECT stored_stone FROM guishi_info WHERE user_id='buyer'"
            )
            order = uow.query_one("SELECT 1 AS present FROM guishi_item WHERE id='q1'")
        self.assertEqual(int(balance["stored_stone"]), 10)
        self.assertIsNotNone(order)

    def test_baitan_refunds_inventory_and_replays(self) -> None:
        first = self.repository.cancel_baitan(
            operation_id="cancel-b1",
            user_id="seller",
            order_id="b1",
            goods_type="药材",
            max_goods_num=10,
        )
        duplicate = self.repository.cancel_baitan(
            operation_id="cancel-b1",
            user_id="seller",
            order_id="b1",
            goods_type="药材",
            max_goods_num=10,
        )

        self.assertEqual((first.status, first.refunded_quantity), ("cancelled", 4))
        self.assertEqual((duplicate.status, duplicate.refunded_quantity), ("duplicate", 4))
        with DatabaseUnitOfWork(self.game_database) as uow:
            inventory = uow.query_one(
                "SELECT goods_num,goods_type FROM back WHERE user_id='seller' AND goods_id=1001"
            )
        with DatabaseUnitOfWork(self.trade_database) as uow:
            order = uow.query_one("SELECT 1 AS present FROM guishi_item WHERE id='b1'")
            operations = uow.query_one(
                "SELECT COUNT(*) AS count FROM guishi_order_cancel_operations"
            )
        self.assertEqual((int(inventory["goods_num"]), inventory["goods_type"]), (8, "药材"))
        self.assertIsNone(order)
        self.assertEqual(int(operations["count"]), 1)

    def test_baitan_inventory_full_keeps_order(self) -> None:
        with DatabaseUnitOfWork(self.game_database) as uow:
            uow.execute(
                "UPDATE back SET goods_num=8 WHERE user_id='seller' AND goods_id=1001"
            )
        result = self.repository.cancel_baitan(
            operation_id="full-b1",
            user_id="seller",
            order_id="b1",
            goods_type="药材",
            max_goods_num=10,
        )
        self.assertEqual(result.status, "inventory_full")
        with DatabaseUnitOfWork(self.trade_database) as uow:
            self.assertIsNotNone(
                uow.query_one("SELECT 1 AS present FROM guishi_item WHERE id='b1'")
            )
            self.assertEqual(
                int(
                    uow.query_one(
                        "SELECT COUNT(*) AS count FROM guishi_order_cancel_operations"
                    )["count"]
                ),
                0,
            )

    def test_baitan_delete_failure_rolls_back_cross_database_refund(self) -> None:
        with DatabaseUnitOfWork(self.trade_database) as uow:
            uow.execute(
                "CREATE TRIGGER reject_baitan_cancel BEFORE DELETE ON guishi_item "
                "WHEN OLD.id='b1' BEGIN SELECT RAISE(ABORT,'reject'); END"
            )
        with self.assertRaises(sqlite3.IntegrityError):
            self.repository.cancel_baitan(
                operation_id="rollback-b1",
                user_id="seller",
                order_id="b1",
                goods_type="药材",
                max_goods_num=10,
            )
        with DatabaseUnitOfWork(self.game_database) as uow:
            inventory = uow.query_one(
                "SELECT goods_num FROM back WHERE user_id='seller' AND goods_id=1001"
            )
        with DatabaseUnitOfWork(self.trade_database) as uow:
            order = uow.query_one("SELECT 1 AS present FROM guishi_item WHERE id='b1'")
        self.assertEqual(int(inventory["goods_num"]), 4)
        self.assertIsNotNone(order)

    def test_trade_cancel_migration_is_trade_db_only(self) -> None:
        migrations = build_migrations()
        game = {
            migration.version
            for migration in migrations_for_database(migrations, "game_db")
        }
        trade = {
            migration.version
            for migration in migrations_for_database(migrations, "trade_db")
        }
        self.assertIn("trade.006", trade)
        self.assertNotIn("trade.006", game)


if __name__ == "__main__":
    unittest.main()
