from __future__ import annotations

import sqlite3
import tempfile
import unittest
from pathlib import Path

from nonebot_plugin_xiuxian_2.features.trade.migrations import apply_trade_xianshi_listing
from nonebot_plugin_xiuxian_2.features.trade.xianshi_listing_repository import (
    XianshiListingSqlRepository,
)
from nonebot_plugin_xiuxian_2.infrastructure.database import DatabaseUnitOfWork


class SequenceIds:
    def __init__(self) -> None:
        self.value = 0

    def new_id(self) -> str:
        self.value += 1
        return f"{self.value:032x}"


class XianshiListingWriteRecoveryTests(unittest.TestCase):
    def setUp(self) -> None:
        self.temp_dir = tempfile.TemporaryDirectory()
        self.database = Path(self.temp_dir.name) / "game.sqlite3"
        with DatabaseUnitOfWork(self.database) as uow:
            uow.execute(
                "CREATE TABLE user_xiuxian ("
                "user_id TEXT PRIMARY KEY, stone INTEGER NOT NULL, user_stamina INTEGER NOT NULL)"
            )
            uow.execute(
                "CREATE TABLE back ("
                "user_id TEXT NOT NULL, goods_id INTEGER NOT NULL, goods_num INTEGER NOT NULL,"
                "bind_num INTEGER NOT NULL DEFAULT 0, state INTEGER NOT NULL DEFAULT 0,"
                "update_time TEXT, action_time TEXT, PRIMARY KEY(user_id,goods_id))"
            )
            uow.execute(
                "CREATE TABLE xianshi_item ("
                "id TEXT PRIMARY KEY,user_id TEXT,goods_id INTEGER,name TEXT,type TEXT,"
                "price INTEGER,quantity INTEGER)"
            )
            uow.execute("INSERT INTO user_xiuxian VALUES ('seller',1000000,100)")
            uow.execute("INSERT INTO back VALUES ('seller',1001,5,0,0,NULL,NULL)")
            apply_trade_xianshi_listing(uow)
        self.repository = XianshiListingSqlRepository(self.database, ids=SequenceIds())

    def tearDown(self) -> None:
        self.temp_dir.cleanup()

    def _rows(self, sql: str):
        with DatabaseUnitOfWork(self.database, read_only=True) as uow:
            return uow.query_all(sql)

    def _execute(self, sql: str) -> None:
        with DatabaseUnitOfWork(self.database) as uow:
            uow.execute(sql)

    def _list(self, operation_id: str = "listing-1", *, stamina_cost: int = 0):
        return self.repository.list_items(
            operation_id,
            "seller",
            1001,
            "法器",
            "装备",
            600000,
            2,
            stamina_cost=stamina_cost,
        )

    def _assert_zero_write(self) -> None:
        self.assertEqual(
            self._rows("SELECT user_id,stone,user_stamina FROM user_xiuxian"),
            [{"user_id": "seller", "stone": 1000000, "user_stamina": 100}],
        )
        self.assertEqual(
            self._rows("SELECT goods_num FROM back"),
            [{"goods_num": 5}],
        )
        self.assertEqual(self._rows("SELECT * FROM xianshi_item"), [])
        self.assertEqual(self._rows("SELECT * FROM xianshi_listing_operations"), [])

    def test_known_write_rejections_roll_back_and_recover(self) -> None:
        cases = (
            (
                "stamina_rewrite",
                "CREATE TRIGGER fault AFTER UPDATE OF user_stamina ON user_xiuxian "
                "BEGIN UPDATE user_xiuxian SET user_stamina=OLD.user_stamina WHERE user_id=NEW.user_id; END",
                "xianshi_listing_stamina_write_rejected",
                "seller",
                10,
            ),
            (
                "stone_rewrite",
                "CREATE TRIGGER fault AFTER UPDATE OF stone ON user_xiuxian "
                "BEGIN UPDATE user_xiuxian SET stone=OLD.stone WHERE user_id=NEW.user_id; END",
                "xianshi_listing_stone_write_rejected",
                "seller",
                0,
            ),
            (
                "stock_rewrite",
                "CREATE TRIGGER fault AFTER UPDATE OF goods_num ON back "
                "BEGIN UPDATE back SET goods_num=OLD.goods_num WHERE user_id=NEW.user_id AND goods_id=NEW.goods_id; END",
                "xianshi_listing_stock_write_rejected",
                "seller:1001",
                0,
            ),
            (
                "item_insert_ignore",
                "CREATE TRIGGER fault BEFORE INSERT ON xianshi_item BEGIN SELECT RAISE(IGNORE); END",
                "xianshi_listing_item_write_rejected",
                "1000000000001",
                0,
            ),
            (
                "item_insert_rewrite",
                "CREATE TRIGGER fault AFTER INSERT ON xianshi_item "
                "BEGIN UPDATE xianshi_item SET quantity=0 WHERE id=NEW.id; END",
                "xianshi_listing_item_write_rejected",
                "1000000000001",
                0,
            ),
            (
                "receipt_insert_ignore",
                "CREATE TRIGGER fault BEFORE INSERT ON xianshi_listing_operations BEGIN SELECT RAISE(IGNORE); END",
                "xianshi_listing_receipt_write_rejected",
                "listing-1",
                0,
            ),
            (
                "receipt_insert_rewrite",
                "CREATE TRIGGER fault AFTER INSERT ON xianshi_listing_operations "
                "BEGIN UPDATE xianshi_listing_operations SET listed_quantity=0 WHERE operation_id=NEW.operation_id; END",
                "xianshi_listing_receipt_write_rejected",
                "listing-1",
                0,
            ),
            (
                "receipt_rewrites_stock",
                "CREATE TRIGGER fault AFTER INSERT ON xianshi_listing_operations "
                "BEGIN UPDATE back SET goods_num=99 WHERE user_id='seller' AND goods_id=1001; END",
                "xianshi_listing_stock_write_rejected",
                "seller:1001",
                0,
            ),
        )
        for name, trigger, reason, subject, stamina_cost in cases:
            with self.subTest(name=name):
                self._execute(trigger)
                rejected = self._list(stamina_cost=stamina_cost)
                self.assertEqual(
                    (rejected.status, rejected.reason, rejected.subject),
                    ("state_changed", reason, subject),
                )
                self._assert_zero_write()
                self._execute("DROP TRIGGER fault")
                applied = self._list(stamina_cost=stamina_cost)
                replay = self._list(stamina_cost=stamina_cost)
                self.assertEqual((applied.status, replay.status), ("listed", "duplicate"))
                self.assertEqual(self._rows("PRAGMA integrity_check"), [{"integrity_check": "ok"}])
                self.tearDown()
                self.setUp()

    def test_system_listing_write_rejections_roll_back_and_recover(self) -> None:
        cases = (
            (
                "item_insert_ignore",
                "CREATE TRIGGER fault BEFORE INSERT ON xianshi_item BEGIN SELECT RAISE(IGNORE); END",
                "xianshi_listing_item_write_rejected",
                "1000000000001",
            ),
            (
                "item_insert_rewrite",
                "CREATE TRIGGER fault AFTER INSERT ON xianshi_item "
                "BEGIN UPDATE xianshi_item SET quantity=0 WHERE id=NEW.id; END",
                "xianshi_listing_item_write_rejected",
                "1000000000001",
            ),
            (
                "receipt_insert_ignore",
                "CREATE TRIGGER fault BEFORE INSERT ON xianshi_listing_operations BEGIN SELECT RAISE(IGNORE); END",
                "xianshi_listing_receipt_write_rejected",
                "system-1",
            ),
            (
                "receipt_insert_rewrite",
                "CREATE TRIGGER fault AFTER INSERT ON xianshi_listing_operations "
                "BEGIN UPDATE xianshi_listing_operations SET listed_quantity=0 WHERE operation_id=NEW.operation_id; END",
                "xianshi_listing_receipt_write_rejected",
                "system-1",
            ),
        )
        for name, trigger, reason, subject in cases:
            with self.subTest(name=name):
                self._execute(trigger)
                rejected = self.repository.list_system_item(
                    "system-1", 1001, "法器", "装备", 600000, -1
                )
                self.assertEqual(
                    (rejected.status, rejected.reason, rejected.subject),
                    ("state_changed", reason, subject),
                )
                self.assertEqual(self._rows("SELECT * FROM xianshi_item"), [])
                self.assertEqual(self._rows("SELECT * FROM xianshi_listing_operations"), [])
                self._execute("DROP TRIGGER fault")
                applied = self.repository.list_system_item(
                    "system-1", 1001, "法器", "装备", 600000, -1
                )
                replay = self.repository.list_system_item(
                    "system-1", 1001, "法器", "装备", 600000, -1
                )
                self.assertEqual((applied.status, replay.status), ("listed", "duplicate"))
                self.assertEqual(self._rows("PRAGMA integrity_check"), [{"integrity_check": "ok"}])
                self.tearDown()
                self.setUp()

    def test_abort_remains_original_exception_and_rolls_back(self) -> None:
        self._execute(
            "CREATE TRIGGER fault BEFORE INSERT ON xianshi_item "
            "BEGIN SELECT RAISE(ABORT,'listing failed'); END"
        )
        with self.assertRaisesRegex(sqlite3.IntegrityError, "listing failed"):
            self._list()
        self._assert_zero_write()

    def test_operation_conflict_keeps_specific_reason(self) -> None:
        self.assertEqual(self._list().status, "listed")
        conflict = self.repository.list_items(
            "listing-1", "seller", 1001, "法器", "装备", 700000, 2
        )
        self.assertEqual((conflict.status, conflict.reason, conflict.subject),
                         ("state_changed", "operation_conflict", "listing-1"))


if __name__ == "__main__":
    unittest.main()
