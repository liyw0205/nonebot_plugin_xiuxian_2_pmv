from __future__ import annotations

import sqlite3
import tempfile
import unittest
from pathlib import Path

from nonebot_plugin_xiuxian_2.features.trade.application import TradeApplication
from nonebot_plugin_xiuxian_2.features.trade.guishi_qiugou_repository import (
    GuishiQiugouSqlRepository,
)
from nonebot_plugin_xiuxian_2.features.trade.migrations import (
    apply_trade_guishi_qiugou,
)
from nonebot_plugin_xiuxian_2.infrastructure.database import DatabaseUnitOfWork
from nonebot_plugin_xiuxian_2.plugin import build_migrations, migrations_for_database


class FixedOrderIds:
    def __init__(self, *values: str) -> None:
        self.values = list(values)

    def new_id(self) -> str:
        return self.values.pop(0)


class GuishiQiugouRepositoryTests(unittest.TestCase):
    def setUp(self) -> None:
        self.temp_dir = tempfile.TemporaryDirectory()
        self.database = Path(self.temp_dir.name) / "trade.db"
        with DatabaseUnitOfWork(self.database) as uow:
            uow.execute(
                "CREATE TABLE guishi_info(user_id TEXT PRIMARY KEY,stored_stone INTEGER,items TEXT)"
            )
            uow.execute(
                "CREATE TABLE guishi_item("
                "id TEXT PRIMARY KEY,user_id TEXT,item_id INTEGER,item_name TEXT,item_type TEXT,"
                "price INTEGER,quantity INTEGER,filled_quantity INTEGER DEFAULT 0)"
            )
            uow.execute(
                "INSERT INTO guishi_info(user_id,stored_stone,items) VALUES(?,?,?)",
                ("buyer", 99, "{}"),
            )
            apply_trade_guishi_qiugou(uow)
        self.repository = GuishiQiugouSqlRepository(
            self.database, order_ids=FixedOrderIds("1001", "1002")
        )

    def tearDown(self) -> None:
        self.temp_dir.cleanup()

    def state(self) -> tuple[int, int, int]:
        with DatabaseUnitOfWork(self.database) as uow:
            balance = int(
                uow.query_one(
                    "SELECT stored_stone FROM guishi_info WHERE user_id='buyer'"
                )["stored_stone"]
            )
            orders = int(
                uow.query_one(
                    "SELECT COUNT(*) AS count FROM guishi_item WHERE item_type='qiugou'"
                )["count"]
            )
            operations = int(
                uow.query_one(
                    "SELECT COUNT(*) AS count FROM guishi_order_create_operations"
                )["count"]
            )
        return balance, orders, operations

    def create(
        self,
        operation_id: str = "create",
        price: int = 20,
        quantity: int = 3,
        max_orders: int = 10,
    ):
        return self.repository.create(
            operation_id=operation_id,
            user_id="buyer",
            item_id=1003,
            item_name="灵石草",
            price=price,
            quantity=quantity,
            max_orders=max_orders,
        )

    def test_creation_freezes_stone_and_replays_without_second_order(self) -> None:
        first = self.create("same")
        replay = self.create("same")
        conflict = self.create("same", price=21)

        self.assertEqual((first.status, replay.status, conflict.status), ("created", "duplicate", "operation_conflict"))
        self.assertEqual((first.total_cost, replay.order_id), (60, first.order_id))
        self.assertEqual(self.state(), (39, 1, 1))

    def test_legacy_operation_payload_replays_without_new_order(self) -> None:
        with DatabaseUnitOfWork(self.database) as uow:
            uow.execute(
                "INSERT INTO guishi_order_create_operations(operation_id,payload,order_id,order_type,amount) "
                "VALUES(?,?,?,?,?)",
                ("legacy", '["buyer", 1003, "灵石草", 20, 3, 10]', "legacy-order", "qiugou", 60),
            )
        replay = self.create("legacy")
        self.assertEqual((replay.status, replay.order_id, replay.total_cost), ("duplicate", "legacy-order", 60))
        self.assertEqual(self.state(), (99, 0, 1))

    def test_limit_and_balance_rejections_do_not_mutate(self) -> None:
        first = self.create("first", price=20, quantity=3)
        limited = self.create("limit", price=1, quantity=1, max_orders=1)
        insufficient = self.create("poor", price=100, quantity=2, max_orders=10)

        self.assertEqual(first.status, "created")
        self.assertEqual(limited.status, "limit_reached")
        self.assertEqual(insufficient.status, "stone_insufficient")
        self.assertEqual(self.state(), (39, 1, 1))

    def test_insert_failure_rolls_back_balance(self) -> None:
        with DatabaseUnitOfWork(self.database) as uow:
            uow.execute(
                "CREATE TRIGGER reject_qiugou BEFORE INSERT ON guishi_item "
                "BEGIN SELECT RAISE(ABORT,'reject'); END"
            )
        with self.assertRaises(sqlite3.IntegrityError):
            self.create("rollback")
        self.assertEqual(self.state(), (99, 0, 0))

    def test_application_default_uses_feature_owned_repository(self) -> None:
        result = TradeApplication(self.database, self.database).guishi_qiugou(
            operation_id="application",
            user_id="buyer",
            item_id=1003,
            item_name="灵石草",
            price=20,
            quantity=3,
            max_orders=10,
        )
        self.assertEqual(result.status, "created")
        self.assertEqual(self.state(), (39, 1, 1))


class GuishiQiugouMigrationRoutingTests(unittest.TestCase):
    def test_operation_schema_is_owned_by_trade_database(self) -> None:
        migrations = build_migrations()
        game = {migration.version for migration in migrations_for_database(migrations, "game_db")}
        trade = {migration.version for migration in migrations_for_database(migrations, "trade_db")}
        self.assertIn("trade.005", trade)
        self.assertNotIn("trade.005", game)


if __name__ == "__main__":
    unittest.main()
