from __future__ import annotations

import json
import sqlite3
import tempfile
import unittest
from datetime import datetime, timezone
from pathlib import Path

from nonebot_plugin_xiuxian_2.features.trade.application import TradeApplication
from nonebot_plugin_xiuxian_2.features.trade.guishi_take_repository import (
    GuishiStoredItemTakeSqlRepository,
)
from nonebot_plugin_xiuxian_2.features.trade.migrations import apply_trade_guishi_take_item
from nonebot_plugin_xiuxian_2.infrastructure.database import DatabaseUnitOfWork
from nonebot_plugin_xiuxian_2.plugin import build_migrations, migrations_for_database


class FixedClock:
    def now(self) -> datetime:
        return datetime(2026, 9, 24, 12, 34, 56, tzinfo=timezone.utc)


class GuishiStoredItemTakeRepositoryTests(unittest.TestCase):
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
            apply_trade_guishi_take_item(uow)
        with DatabaseUnitOfWork(self.trade_database) as uow:
            uow.execute(
                "CREATE TABLE guishi_info(user_id TEXT PRIMARY KEY,stored_stone INTEGER,items TEXT)"
            )
            uow.execute("INSERT INTO guishi_info VALUES('user',15,'{\"1001\":3}')")
        self.repository = GuishiStoredItemTakeSqlRepository(
            self.game_database, self.trade_database, clock=FixedClock()
        )

    def tearDown(self) -> None:
        self.temp_dir.cleanup()

    def take(self, operation_id: str = "take-1", *, goods_id: int = 1001):
        return self.repository.take(
            operation_id=operation_id,
            user_id="user",
            goods_id=goods_id,
            item_name="测试法器",
            goods_type="装备",
            max_goods_num=99,
        )

    def state(self):
        with DatabaseUnitOfWork(self.trade_database) as uow:
            account = uow.query_one("SELECT stored_stone,items FROM guishi_info WHERE user_id='user'")
        with DatabaseUnitOfWork(self.game_database) as uow:
            inventory = uow.query_one("SELECT goods_num,bind_num FROM back WHERE user_id='user' AND goods_id=1001")
            operations = uow.query_one("SELECT COUNT(*) AS count FROM guishi_take_item_operations")
        return (
            int(account["stored_stone"]),
            json.loads(account["items"]),
            None if inventory is None else (int(inventory["goods_num"]), int(inventory["bind_num"])),
            int(operations["count"]),
        )

    def test_take_moves_storage_to_bound_inventory_and_replays_once(self) -> None:
        first = self.take("take-once")
        duplicate = self.take("take-once")
        conflict = self.take("take-once", goods_id=1002)
        self.assertEqual((first.status, first.quantity), ("taken", 3))
        self.assertEqual((duplicate.status, duplicate.quantity), ("duplicate", 3))
        self.assertEqual(conflict.status, "operation_conflict")
        self.assertEqual(self.state(), (15, {}, (3, 3), 1))

    def test_old_operation_row_is_replayed_without_second_delivery(self) -> None:
        with DatabaseUnitOfWork(self.game_database) as uow:
            uow.execute(
                "INSERT INTO guishi_take_item_operations(operation_id,user_id,goods_id,"
                "item_name,goods_type,quantity) VALUES(?,?,?,?,?,?)",
                ("legacy-take", "user", 1001, "测试法器", "装备", 3),
            )
        result = self.take("legacy-take")
        self.assertEqual((result.status, result.quantity), ("duplicate", 3))
        self.assertEqual(self.state(), (15, {"1001": 3}, None, 1))

    def test_inventory_full_keeps_storage_and_operation_absent(self) -> None:
        with DatabaseUnitOfWork(self.game_database) as uow:
            uow.execute(
                "INSERT INTO back VALUES('user',1001,'测试法器','装备',98,NULL,NULL,0)"
            )
        result = self.take("full-take")
        self.assertEqual(result.status, "inventory_full")
        self.assertEqual(self.state(), (15, {"1001": 3}, (98, 0), 0))

    def test_operation_insert_failure_rolls_back_storage_and_inventory(self) -> None:
        with DatabaseUnitOfWork(self.game_database) as uow:
            uow.execute(
                "CREATE TRIGGER reject_take BEFORE INSERT ON guishi_take_item_operations "
                "BEGIN SELECT RAISE(ABORT,'reject'); END"
            )
        with self.assertRaises(sqlite3.IntegrityError):
            self.take("failed-take")
        self.assertEqual(self.state(), (15, {"1001": 3}, None, 0))

    def test_application_owns_take_and_migration_is_game_db_only(self) -> None:
        application = TradeApplication(
            self.game_database, self.trade_database, clock=FixedClock()
        )
        result = application.guishi_take_stored_item(
            operation_id="application-take",
            user_id="user",
            goods_id=1001,
            item_name="测试法器",
            goods_type="装备",
            max_goods_num=99,
        )
        self.assertTrue(result.succeeded)
        migrations = build_migrations()
        game = {item.version for item in migrations_for_database(migrations, "game_db")}
        trade = {item.version for item in migrations_for_database(migrations, "trade_db")}
        self.assertIn("trade.009", game)
        self.assertNotIn("trade.009", trade)


if __name__ == "__main__":
    unittest.main()
