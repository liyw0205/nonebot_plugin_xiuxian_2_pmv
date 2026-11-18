from __future__ import annotations

import sqlite3
import tempfile
import unittest
from pathlib import Path

from nonebot_plugin_xiuxian_2.core.result import OperationOutcome
from nonebot_plugin_xiuxian_2.features.trade.application import TradeApplication
from nonebot_plugin_xiuxian_2.features.trade.migrations import apply_trade_xianshi_purchase
from nonebot_plugin_xiuxian_2.features.trade.xianshi_purchase_repository import (
    XianshiPurchaseSqlRepository,
)
from nonebot_plugin_xiuxian_2.infrastructure.database import DatabaseUnitOfWork
from nonebot_plugin_xiuxian_2.plugin import apply_platform_schema
from nonebot_plugin_xiuxian_2.xiuxian.command_guard import result_failure


class XianshiPurchaseWriteRecoveryTests(unittest.TestCase):
    def setUp(self) -> None:
        self.temp_dir = tempfile.TemporaryDirectory()
        self.database = Path(self.temp_dir.name) / "game.sqlite3"
        with DatabaseUnitOfWork(self.database) as uow:
            apply_platform_schema(uow)
            apply_trade_xianshi_purchase(uow)
            uow.execute(
                "CREATE TABLE user_xiuxian ("
                "user_id TEXT PRIMARY KEY, stone INTEGER NOT NULL, user_stamina INTEGER NOT NULL)"
            )
            uow.execute(
                "CREATE TABLE back ("
                "user_id TEXT NOT NULL, goods_id INTEGER NOT NULL, goods_name TEXT, goods_type TEXT,"
                "goods_num INTEGER DEFAULT 0, create_time TEXT, update_time TEXT, bind_num INTEGER DEFAULT 0,"
                "PRIMARY KEY(user_id,goods_id))"
            )
            uow.executemany(
                "INSERT INTO user_xiuxian(user_id,stone,user_stamina) VALUES(?,?,?)",
                (("buyer", 1000, 20), ("seller", 100, 20)),
            )
            uow.execute(
                "INSERT INTO xianshi_item(id,user_id,goods_id,name,type,price,quantity) "
                "VALUES('listing-1','seller',1001,'法器','装备',200,2)"
            )

    def tearDown(self) -> None:
        self.temp_dir.cleanup()

    def _execute(self, sql: str) -> None:
        with DatabaseUnitOfWork(self.database) as uow:
            uow.execute(sql)

    def _rows(self, sql: str):
        with DatabaseUnitOfWork(self.database, read_only=True) as uow:
            return uow.query_all(sql)

    def _purchase(self, *, operation_id="purchase-1", quantity=1, stamina=True):
        return XianshiPurchaseSqlRepository(self.database).purchase(
            operation_id,
            "buyer",
            "listing-1",
            quantity,
            max_goods_num=99,
            stamina_operation_id="batch-1" if stamina else None,
            stamina_cost=10 if stamina else 0,
        )

    def _assert_zero_write(self) -> None:
        self.assertEqual(
            [(row["user_id"], row["stone"], row["user_stamina"]) for row in self._rows(
                "SELECT user_id,stone,user_stamina FROM user_xiuxian ORDER BY user_id"
            )],
            [("buyer", 1000, 20), ("seller", 100, 20)],
        )
        self.assertEqual(
            self._rows("SELECT id,quantity FROM xianshi_item"),
            [{"id": "listing-1", "quantity": 2}],
        )
        self.assertEqual(self._rows("SELECT * FROM back"), [])
        self.assertEqual(self._rows("SELECT * FROM xianshi_operations"), [])
        self.assertEqual(self._rows("SELECT * FROM xianshi_stamina_operations"), [])

    def test_known_write_rejections_roll_back_and_recover_with_same_operation(self):
        cases = (
            (
                "listing_delete_ignore",
                "CREATE TRIGGER fault BEFORE DELETE ON xianshi_item BEGIN SELECT RAISE(IGNORE); END",
                2,
                "xianshi_purchase_listing_write_rejected",
                "listing-1",
            ),
            (
                "listing_update_rewrite",
                "CREATE TRIGGER fault AFTER UPDATE OF quantity ON xianshi_item "
                "BEGIN UPDATE xianshi_item SET quantity=OLD.quantity WHERE id=OLD.id; END",
                1,
                "xianshi_purchase_listing_write_rejected",
                "listing-1",
            ),
            (
                "inventory_insert_ignore",
                "CREATE TRIGGER fault BEFORE INSERT ON back BEGIN SELECT RAISE(IGNORE); END",
                1,
                "xianshi_purchase_inventory_write_rejected",
                "buyer:1001",
            ),
            (
                "inventory_insert_rewrite",
                "CREATE TRIGGER fault AFTER INSERT ON back "
                "BEGIN UPDATE back SET goods_num=0 WHERE user_id=NEW.user_id AND goods_id=NEW.goods_id; END",
                1,
                "xianshi_purchase_inventory_write_rejected",
                "buyer:1001",
            ),
            (
                "stamina_update_rewrite",
                "CREATE TRIGGER fault AFTER UPDATE OF user_stamina ON user_xiuxian "
                "WHEN OLD.user_id='buyer' BEGIN UPDATE user_xiuxian SET user_stamina=OLD.user_stamina WHERE user_id='buyer'; END",
                1,
                "xianshi_purchase_stamina_write_rejected",
                "buyer",
            ),
            (
                "stamina_receipt_ignore",
                "CREATE TRIGGER fault BEFORE INSERT ON xianshi_stamina_operations BEGIN SELECT RAISE(IGNORE); END",
                1,
                "xianshi_purchase_stamina_receipt_write_rejected",
                "batch-1",
            ),
            (
                "stamina_receipt_rewrite",
                "CREATE TRIGGER fault AFTER INSERT ON xianshi_stamina_operations "
                "BEGIN UPDATE xianshi_stamina_operations SET stamina_cost=0 WHERE operation_id=NEW.operation_id; END",
                1,
                "xianshi_purchase_stamina_receipt_write_rejected",
                "batch-1",
            ),
            (
                "purchase_receipt_ignore",
                "CREATE TRIGGER fault BEFORE INSERT ON xianshi_operations BEGIN SELECT RAISE(IGNORE); END",
                1,
                "xianshi_purchase_receipt_write_rejected",
                "purchase-1",
            ),
            (
                "purchase_receipt_rewrite",
                "CREATE TRIGGER fault AFTER INSERT ON xianshi_operations "
                "BEGIN UPDATE xianshi_operations SET quantity=99 WHERE operation_id=NEW.operation_id; END",
                1,
                "xianshi_purchase_receipt_write_rejected",
                "purchase-1",
            ),
        )
        for name, trigger, quantity, reason, subject in cases:
            with self.subTest(name=name):
                self._execute(trigger)
                rejected = self._purchase(quantity=quantity)
                self.assertEqual((rejected.status, rejected.reason, rejected.subject),
                                 ("state_changed", reason, subject))
                self._assert_zero_write()
                self._execute("DROP TRIGGER fault")
                applied = self._purchase(quantity=quantity)
                replay = self._purchase(quantity=quantity)
                self.assertEqual((applied.status, replay.status), ("purchased", "duplicate"))
                self.assertEqual(self._rows("PRAGMA integrity_check"), [{"integrity_check": "ok"}])
                # A fresh fixture is required for the next trigger case.
                self.tearDown()
                self.setUp()

    def test_abort_remains_original_exception_and_rolls_back(self):
        self._execute(
            "CREATE TRIGGER fault BEFORE INSERT ON xianshi_operations "
            "BEGIN SELECT RAISE(ABORT, 'purchase receipt failed'); END"
        )
        with self.assertRaisesRegex(sqlite3.IntegrityError, "purchase receipt failed"):
            self._purchase()
        self._assert_zero_write()

    def test_application_and_player_reason_keep_specific_rejection(self):
        self._execute("CREATE TRIGGER fault BEFORE INSERT ON back BEGIN SELECT RAISE(IGNORE); END")
        outcome = TradeApplication(self.database, self.database).purchase(
            operation_id="application-purchase",
            user_id="buyer",
            listing_id="listing-1",
            quantity=1,
            max_goods_num=99,
            stamina_operation_id="batch-application",
            stamina_cost=10,
        )
        self.assertIsInstance(outcome, OperationOutcome)
        self.assertEqual((outcome.status, outcome.code), ("rejected", "state_changed"))
        self.assertEqual(outcome.data["reason"], "xianshi_purchase_inventory_write_rejected")
        self.assertIn("物品入背包失败", result_failure("仙肆购买", outcome))
        self._assert_zero_write()


if __name__ == "__main__":
    unittest.main()
