from __future__ import annotations

import tempfile
import unittest
from pathlib import Path

from nonebot_plugin_xiuxian_2.features.trade.migrations import (
    apply_trade_xianshi_listing,
    apply_trade_xianshi_removal,
)
from nonebot_plugin_xiuxian_2.features.trade.xianshi_removal_repository import (
    XianshiRemovalSqlRepository,
)
from nonebot_plugin_xiuxian_2.infrastructure.database import DatabaseUnitOfWork


class XianshiRemovalWriteRecoveryTests(unittest.TestCase):
    def setUp(self) -> None:
        self.temp_dir = tempfile.TemporaryDirectory()
        self.database = Path(self.temp_dir.name) / "game.sqlite3"
        with DatabaseUnitOfWork(self.database) as uow:
            uow.execute(
                "CREATE TABLE back (user_id TEXT,goods_id INTEGER,goods_name TEXT,"
                "goods_type TEXT,goods_num INTEGER DEFAULT 0,create_time TEXT,"
                "update_time TEXT,bind_num INTEGER DEFAULT 0,state INTEGER DEFAULT 0,"
                "action_time TEXT,UNIQUE(user_id,goods_id))"
            )
            uow.execute(
                "CREATE TABLE xianshi_item (id TEXT PRIMARY KEY,user_id TEXT,goods_id INTEGER,"
                "name TEXT,type TEXT,price INTEGER,quantity INTEGER)"
            )
            apply_trade_xianshi_listing(uow)
            apply_trade_xianshi_removal(uow)
        self.repository = XianshiRemovalSqlRepository(self.database)

    def tearDown(self) -> None:
        self.temp_dir.cleanup()

    def reset(self) -> None:
        self.tearDown()
        self.setUp()

    def execute(self, sql: str) -> None:
        with DatabaseUnitOfWork(self.database) as uow:
            uow.execute(sql)

    def rows(self, sql: str):
        with DatabaseUnitOfWork(self.database, read_only=True) as uow:
            return uow.query_all(sql)

    def insert_listing(
        self,
        listing_id: str = "listing-1",
        seller_id: str = "seller",
        goods_id: int = 1001,
        name: str = "法器",
        goods_type: str = "装备",
        quantity: int = 2,
        price: int = 600000,
    ) -> None:
        with DatabaseUnitOfWork(self.database) as uow:
            uow.execute(
                "INSERT INTO xianshi_item VALUES(?,?,?,?,?,?,?)",
                (listing_id, seller_id, goods_id, name, goods_type, price, quantity),
            )

    def assert_zero_write(self) -> None:
        self.assertEqual(self.rows("SELECT * FROM xianshi_item"), [
            {
                "id": "listing-1", "user_id": "seller", "goods_id": 1001,
                "name": "法器", "type": "装备", "price": 600000, "quantity": 2,
            }
        ])
        self.assertEqual(self.rows("SELECT * FROM back"), [])
        self.assertEqual(self.rows("SELECT * FROM xianshi_removal_operations"), [])

    def test_single_removal_rejects_each_known_write_and_recovers(self) -> None:
        cases = (
            (
                "refund_insert_ignore",
                "CREATE TRIGGER fault BEFORE INSERT ON back WHEN NEW.user_id='seller' "
                "BEGIN SELECT RAISE(IGNORE); END",
                "xianshi_removal_inventory_write_rejected",
                "seller:1001",
            ),
            (
                "listing_delete_ignore",
                "CREATE TRIGGER fault BEFORE DELETE ON xianshi_item "
                "BEGIN SELECT RAISE(IGNORE); END",
                "xianshi_removal_listing_write_rejected",
                "listing-1",
            ),
            (
                "receipt_insert_ignore",
                "CREATE TRIGGER fault BEFORE INSERT ON xianshi_removal_operations "
                "BEGIN SELECT RAISE(IGNORE); END",
                "xianshi_removal_receipt_write_rejected",
                "remove-1",
            ),
            (
                "receipt_rewrites_inventory",
                "CREATE TRIGGER fault AFTER INSERT ON xianshi_removal_operations BEGIN "
                "UPDATE back SET goods_num=99 WHERE user_id='seller' AND goods_id=1001; END",
                "xianshi_removal_inventory_write_rejected",
                "seller:1001",
            ),
        )
        for name, trigger, reason, subject in cases:
            with self.subTest(name=name):
                self.insert_listing()
                self.execute(trigger)
                rejected = self.repository.remove_listing(
                    "remove-1", "listing-1", max_goods_num=10
                )
                self.assertEqual(
                    (rejected.status, rejected.reason, rejected.subject),
                    ("state_changed", reason, subject),
                )
                self.assert_zero_write()
                self.execute("DROP TRIGGER fault")
                applied = self.repository.remove_listing(
                    "remove-1", "listing-1", max_goods_num=10
                )
                replay = self.repository.remove_listing(
                    "remove-1", "listing-1", max_goods_num=10
                )
                self.assertEqual((applied.status, replay.status), ("removed", "duplicate"))
                self.assertEqual(self.rows("SELECT goods_num FROM back"), [{"goods_num": 2}])
                self.assertEqual(self.rows("PRAGMA integrity_check"), [{"integrity_check": "ok"}])
                self.reset()

    def test_existing_inventory_update_rewrite_is_rejected(self) -> None:
        self.insert_listing()
        self.execute(
            "INSERT INTO back(user_id,goods_id,goods_num) VALUES('seller',1001,1)"
        )
        self.execute(
            "CREATE TRIGGER fault AFTER UPDATE ON back WHEN NEW.user_id='seller' "
            "BEGIN UPDATE back SET goods_num=OLD.goods_num WHERE user_id=NEW.user_id AND goods_id=NEW.goods_id; END"
        )
        rejected = self.repository.remove_listing("remove-1", "listing-1", max_goods_num=10)
        self.assertEqual(
            (rejected.status, rejected.reason, rejected.subject),
            ("state_changed", "xianshi_removal_inventory_write_rejected", "seller:1001"),
        )
        self.assertEqual(self.rows("SELECT goods_num FROM back"), [{"goods_num": 1}])
        self.assertEqual(self.rows("SELECT quantity FROM xianshi_item"), [{"quantity": 2}])
        self.assertEqual(self.rows("SELECT * FROM xianshi_removal_operations"), [])

    def test_name_removal_rejects_partial_listing_write_and_receipt(self) -> None:
        cases = (
            (
                "partial_update_ignore",
                "CREATE TRIGGER fault BEFORE UPDATE ON xianshi_item "
                "BEGIN SELECT RAISE(IGNORE); END",
                "xianshi_name_removal_listing_write_rejected",
                "listing-1",
                1,
            ),
            (
                "delete_ignore",
                "CREATE TRIGGER fault BEFORE DELETE ON xianshi_item "
                "BEGIN SELECT RAISE(IGNORE); END",
                "xianshi_name_removal_listing_write_rejected",
                "listing-1",
                2,
            ),
            (
                "receipt_ignore",
                "CREATE TRIGGER fault BEFORE INSERT ON xianshi_name_removal_operations "
                "BEGIN SELECT RAISE(IGNORE); END",
                "xianshi_name_removal_receipt_write_rejected",
                "remove-name-1",
                1,
            ),
        )
        for name, trigger, reason, subject, requested in cases:
            with self.subTest(name=name):
                self.insert_listing(quantity=2)
                self.execute(trigger)
                rejected = self.repository.remove_by_name(
                    "remove-name-1", "seller", "法器", requested, max_goods_num=10
                )
                self.assertEqual(
                    (rejected.status, rejected.reason, rejected.subject),
                    ("state_changed", reason, subject),
                )
                self.assert_zero_write()
                self.execute("DROP TRIGGER fault")
                applied = self.repository.remove_by_name(
                    "remove-name-1", "seller", "法器", requested, max_goods_num=10
                )
                replay = self.repository.remove_by_name(
                    "remove-name-1", "seller", "法器", requested, max_goods_num=10
                )
                self.assertEqual((applied.status, replay.status), ("removed", "duplicate"))
                expected_remaining = 2 - requested
                self.assertEqual(
                    self.rows("SELECT quantity FROM xianshi_item"),
                    ([{"quantity": expected_remaining}] if expected_remaining else []),
                )
                self.assertEqual(self.rows("SELECT goods_num FROM back"), [{"goods_num": requested}])
                self.reset()

    def test_clear_rejects_refund_delete_receipt_and_restores_all_rows(self) -> None:
        cases = (
            (
                "refund_ignore",
                "CREATE TRIGGER fault BEFORE INSERT ON back WHEN NEW.user_id='seller' "
                "BEGIN SELECT RAISE(IGNORE); END",
                "xianshi_clear_inventory_write_rejected",
                "seller:1001",
            ),
            (
                "delete_ignore",
                "CREATE TRIGGER fault BEFORE DELETE ON xianshi_item "
                "BEGIN SELECT RAISE(IGNORE); END",
                "xianshi_clear_listing_write_rejected",
                "xianshi_item",
            ),
            (
                "receipt_ignore",
                "CREATE TRIGGER fault BEFORE INSERT ON xianshi_clear_operations "
                "BEGIN SELECT RAISE(IGNORE); END",
                "xianshi_clear_receipt_write_rejected",
                "clear-1",
            ),
            (
                "receipt_rewrites_inventory",
                "CREATE TRIGGER fault AFTER INSERT ON xianshi_clear_operations BEGIN "
                "UPDATE back SET goods_num=99 WHERE user_id='seller' AND goods_id=1001; END",
                "xianshi_clear_inventory_write_rejected",
                "seller:1001",
            ),
        )
        for name, trigger, reason, subject in cases:
            with self.subTest(name=name):
                self.insert_listing(quantity=2)
                self.execute(trigger)
                rejected = self.repository.clear_all("clear-1", max_goods_num=10)
                self.assertEqual(
                    (rejected.status, rejected.reason, rejected.subject),
                    ("state_changed", reason, subject),
                )
                self.assert_zero_write()
                self.execute("DROP TRIGGER fault")
                applied = self.repository.clear_all("clear-1", max_goods_num=10)
                replay = self.repository.clear_all("clear-1", max_goods_num=10)
                self.assertEqual((applied.status, replay.status), ("cleared", "duplicate"))
                self.assertEqual(self.rows("SELECT COUNT(*) AS count FROM xianshi_item"), [{"count": 0}])
                self.assertEqual(self.rows("SELECT goods_num FROM back"), [{"goods_num": 2}])
                self.assertEqual(self.rows("PRAGMA integrity_check"), [{"integrity_check": "ok"}])
                self.reset()


if __name__ == "__main__":
    unittest.main()
