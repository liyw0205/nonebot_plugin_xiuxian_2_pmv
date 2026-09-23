from __future__ import annotations

import sqlite3
import tempfile
import unittest
from datetime import datetime, timezone
from pathlib import Path

from nonebot_plugin_xiuxian_2.features.trade.application import TradeApplication
from nonebot_plugin_xiuxian_2.features.trade.guishi_baitan_repository import (
    GuishiBaitanSqlRepository,
)
from nonebot_plugin_xiuxian_2.features.trade.migrations import apply_trade_guishi_qiugou
from nonebot_plugin_xiuxian_2.infrastructure.database import DatabaseUnitOfWork


class FixedOrderIds:
    def __init__(self, *values: str) -> None:
        self.values = list(values)

    def new_id(self) -> str:
        return self.values.pop(0)


class FixedClock:
    def now(self) -> datetime:
        return datetime(2026, 9, 24, 12, 34, 56, tzinfo=timezone.utc)


class GuishiBaitanRepositoryTests(unittest.TestCase):
    def setUp(self) -> None:
        self.temp_dir = tempfile.TemporaryDirectory()
        root = Path(self.temp_dir.name)
        self.game_database = root / "game.db"
        self.trade_database = root / "trade.db"
        with DatabaseUnitOfWork(self.game_database) as uow:
            uow.execute(
                "CREATE TABLE back("
                "user_id TEXT,goods_id INTEGER,goods_num INTEGER,bind_num INTEGER DEFAULT 0,"
                "state INTEGER DEFAULT 0,update_time TEXT,UNIQUE(user_id,goods_id))"
            )
            uow.execute(
                "INSERT INTO back(user_id,goods_id,goods_num,bind_num,state) VALUES(?,?,?,?,?)",
                ("seller", 1001, 8, 2, 1),
            )
        with DatabaseUnitOfWork(self.trade_database) as uow:
            uow.execute(
                "CREATE TABLE guishi_info(user_id TEXT PRIMARY KEY,stored_stone INTEGER,items TEXT)"
            )
            uow.execute(
                "CREATE TABLE guishi_item("
                "id TEXT PRIMARY KEY,user_id TEXT,item_id INTEGER,item_name TEXT,item_type TEXT,"
                "price INTEGER,quantity INTEGER,filled_quantity INTEGER DEFAULT 0)"
            )
            apply_trade_guishi_qiugou(uow)
        self.repository = GuishiBaitanSqlRepository(
            self.game_database,
            self.trade_database,
            order_ids=FixedOrderIds("1001", "1002"),
            clock=FixedClock(),
        )

    def tearDown(self) -> None:
        self.temp_dir.cleanup()

    def state(self) -> tuple[int, int, int]:
        with DatabaseUnitOfWork(self.game_database) as uow:
            inventory = uow.query_one(
                "SELECT goods_num,bind_num FROM back WHERE user_id='seller' AND goods_id=1001"
            )
        with DatabaseUnitOfWork(self.trade_database) as uow:
            orders = uow.query_one(
                "SELECT COUNT(*) AS count FROM guishi_item WHERE item_type='baitan'"
            )
            operations = uow.query_one(
                "SELECT COUNT(*) AS count FROM guishi_order_create_operations"
            )
        return int(inventory["goods_num"]), int(inventory["bind_num"]), int(orders["count"]) + int(operations["count"])

    def create(
        self,
        operation_id: str = "create",
        quantity: int = 4,
        max_orders: int = 10,
    ):
        return self.repository.create(
            operation_id=operation_id,
            user_id="seller",
            item_id=1001,
            item_name="灵草",
            price=20,
            quantity=quantity,
            max_orders=max_orders,
        )

    def test_creation_reserves_tradeable_inventory_and_replays(self) -> None:
        first = self.create("same")
        replay = self.create("same")
        conflict = self.repository.create(
            operation_id="same",
            user_id="seller",
            item_id=1001,
            item_name="灵草",
            price=21,
            quantity=4,
            max_orders=10,
        )

        self.assertEqual((first.status, replay.status, conflict.status), ("created", "duplicate", "operation_conflict"))
        self.assertEqual((first.quantity, replay.order_id), (4, first.order_id))
        self.assertEqual(self.state(), (4, 2, 2))

    def test_limit_and_tradeable_stock_rejections_do_not_mutate(self) -> None:
        first = self.create("first", quantity=4)
        limited = self.create("limit", quantity=1, max_orders=1)
        unavailable = self.create("poor", quantity=4, max_orders=10)

        self.assertEqual(first.status, "created")
        self.assertEqual(limited.status, "limit_reached")
        self.assertEqual(unavailable.status, "stock_insufficient")
        self.assertEqual(self.state(), (4, 2, 2))

    def test_insert_failure_rolls_back_inventory(self) -> None:
        with DatabaseUnitOfWork(self.trade_database) as uow:
            uow.execute(
                "CREATE TRIGGER reject_baitan BEFORE INSERT ON guishi_item "
                "WHEN NEW.item_type='baitan' BEGIN SELECT RAISE(ABORT,'reject'); END"
            )
        with self.assertRaises(sqlite3.IntegrityError):
            self.create("rollback")
        self.assertEqual(self.state(), (8, 2, 0))

    def test_order_id_conflict_retries(self) -> None:
        with DatabaseUnitOfWork(self.trade_database) as uow:
            uow.execute(
                "INSERT INTO guishi_item(id,user_id,item_id,item_name,item_type,price,quantity) "
                "VALUES(?,?,?,?,?,?,?)",
                ("1001", "other", 1001, "灵草", "baitan", 20, 1),
            )
        result = self.create("retry", quantity=2)
        self.assertEqual(result.status, "created")
        self.assertEqual(result.order_id, "1002")

    def test_application_default_uses_feature_owned_repository(self) -> None:
        result = TradeApplication(
            self.game_database,
            self.trade_database,
            clock=FixedClock(),
        ).guishi_baitan(
            operation_id="application",
            user_id="seller",
            item_id=1001,
            item_name="灵草",
            price=20,
            quantity=3,
            max_orders=10,
        )
        self.assertEqual(result.status, "created")
        self.assertEqual(self.state(), (5, 2, 2))


if __name__ == "__main__":
    unittest.main()
