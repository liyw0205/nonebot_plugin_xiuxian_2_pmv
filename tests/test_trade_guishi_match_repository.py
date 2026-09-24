from __future__ import annotations

import json
import sqlite3
import tempfile
import unittest
from pathlib import Path

from nonebot_plugin_xiuxian_2.features.trade.application import TradeApplication
from nonebot_plugin_xiuxian_2.features.trade.guishi_match_repository import (
    GuishiOrderMatchSqlRepository,
)
from nonebot_plugin_xiuxian_2.features.trade.migrations import apply_trade_guishi_match
from nonebot_plugin_xiuxian_2.infrastructure.database import DatabaseUnitOfWork
from nonebot_plugin_xiuxian_2.plugin import build_migrations, migrations_for_database


class GuishiMatchRepositoryTests(unittest.TestCase):
    def setUp(self) -> None:
        self.temp_dir = tempfile.TemporaryDirectory()
        self.database = Path(self.temp_dir.name) / "trade.db"
        with DatabaseUnitOfWork(self.database) as uow:
            uow.execute(
                "CREATE TABLE guishi_info(user_id TEXT PRIMARY KEY,stored_stone INTEGER,items TEXT)"
            )
            uow.execute(
                "CREATE TABLE guishi_item(id TEXT PRIMARY KEY,user_id TEXT,item_id INTEGER,"
                "item_name TEXT,item_type TEXT,price INTEGER,quantity INTEGER,"
                "filled_quantity INTEGER DEFAULT 0)"
            )
            uow.execute("INSERT INTO guishi_info VALUES('buyer',99,'{}')")
            uow.execute("INSERT INTO guishi_info VALUES('seller',17,'{}')")
            uow.execute(
                "INSERT INTO guishi_item VALUES('want','buyer',1001,'灵草','qiugou',30,8,2)"
            )
            uow.execute(
                "INSERT INTO guishi_item VALUES('sell','seller',1001,'灵草','baitan',20,10,6)"
            )
            apply_trade_guishi_match(uow)
        self.repository = GuishiOrderMatchSqlRepository(self.database)

    def tearDown(self) -> None:
        self.temp_dir.cleanup()

    def _info(self, user_id: str):
        with DatabaseUnitOfWork(self.database) as uow:
            row = uow.query_one("SELECT stored_stone,items FROM guishi_info WHERE user_id=?", (user_id,))
            return int(row["stored_stone"]), json.loads(row["items"])

    def test_match_and_replay_are_atomic(self) -> None:
        with DatabaseUnitOfWork(self.database) as uow:
            uow.execute("UPDATE guishi_item SET quantity=12 WHERE id='sell'")
        first = self.repository.match(
            operation_id="match-1", qiugou_order_id="want", baitan_order_id="sell"
        )
        duplicate = self.repository.match(
            operation_id="match-1", qiugou_order_id="want", baitan_order_id="sell"
        )
        conflict = self.repository.match(
            operation_id="match-1", qiugou_order_id="other", baitan_order_id="sell"
        )
        self.assertEqual((first.status, first.quantity, first.amount), ("matched", 6, 120))
        self.assertEqual((duplicate.status, duplicate.quantity), ("duplicate", 6))
        self.assertEqual(conflict.status, "operation_conflict")
        self.assertEqual(self._info("buyer"), (99, {"1001": 6}))
        self.assertEqual(self._info("seller"), (137, {}))

    def test_partial_match_and_rollback(self) -> None:
        with DatabaseUnitOfWork(self.database) as uow:
            uow.execute("UPDATE guishi_item SET quantity=12 WHERE id='want'")
            uow.execute("UPDATE guishi_item SET quantity=12 WHERE id='sell'")
        result = self.repository.match(
            operation_id="partial", qiugou_order_id="want", baitan_order_id="sell"
        )
        self.assertEqual((result.quantity, result.qiugou_completed, result.baitan_completed), (6, False, True))
        with DatabaseUnitOfWork(self.database) as uow:
            row = uow.query_one("SELECT quantity,filled_quantity FROM guishi_item WHERE id='want'")
        self.assertEqual((int(row["quantity"]), int(row["filled_quantity"])), (12, 8))

        with DatabaseUnitOfWork(self.database) as uow:
            uow.execute(
                "INSERT INTO guishi_item VALUES('sell2','seller',1001,'灵草','baitan',20,2,0)"
            )
            uow.execute(
                "CREATE TRIGGER reject_seller BEFORE UPDATE ON guishi_info "
                "WHEN NEW.user_id='seller' BEGIN SELECT RAISE(ABORT,'reject'); END"
            )
        with self.assertRaises(sqlite3.IntegrityError):
            self.repository.match(
                operation_id="rollback", qiugou_order_id="want", baitan_order_id="sell2"
            )

    def test_legacy_match_operation_replays_without_settling_twice(self) -> None:
        legacy_result = {
            "status": "matched",
            "qiugou_order_id": "want",
            "baitan_order_id": "sell",
            "buyer_id": "buyer",
            "seller_id": "seller",
            "item_id": 1001,
            "item_name": "灵草",
            "quantity": 4,
            "amount": 80,
            "qiugou_completed": False,
            "baitan_completed": True,
        }
        with DatabaseUnitOfWork(self.database) as uow:
            uow.execute(
                "INSERT INTO guishi_match_operations(operation_id,payload,result) VALUES(?,?,?)",
                (
                    "guishi-match:want:sell",
                    json.dumps(["want", "sell"]),
                    json.dumps(legacy_result, ensure_ascii=False),
                ),
            )
        result = self.repository.match(
            operation_id="guishi-match:want:sell",
            qiugou_order_id="want",
            baitan_order_id="sell",
        )
        self.assertEqual((result.status, result.quantity, result.amount), ("duplicate", 4, 80))
        self.assertEqual(self._info("buyer"), (99, {}))
        self.assertEqual(self._info("seller"), (17, {}))

    def test_application_owns_matching_and_trade_migration_is_routed(self) -> None:
        with DatabaseUnitOfWork(self.database) as uow:
            uow.execute("UPDATE guishi_item SET quantity=12 WHERE id='sell'")
        application = TradeApplication(self.database, self.database)
        result = application.guishi_match(
            operation_id="app-match", qiugou_order_id="want", baitan_order_id="sell"
        )
        self.assertTrue(result.matched)
        migrations = build_migrations()
        trade = {item.version for item in migrations_for_database(migrations, "trade_db")}
        game = {item.version for item in migrations_for_database(migrations, "game_db")}
        self.assertIn("trade.007", trade)
        self.assertNotIn("trade.007", game)


if __name__ == "__main__":
    unittest.main()
