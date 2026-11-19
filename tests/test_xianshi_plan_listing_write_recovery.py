from __future__ import annotations

import sqlite3
import tempfile
import unittest
from pathlib import Path

from nonebot_plugin_xiuxian_2.features.trade.migrations import (
    apply_trade_xianshi_listing,
    apply_trade_xianshi_plan_listing,
)
from nonebot_plugin_xiuxian_2.features.trade.xianshi_plan_listing_repository import (
    XianshiPlanListingSqlRepository,
)
from nonebot_plugin_xiuxian_2.infrastructure.database import DatabaseUnitOfWork


class SequenceIds:
    def __init__(self) -> None:
        self.value = 0

    def new_id(self) -> str:
        self.value += 1
        return f"{self.value:032x}"


class XianshiPlanListingWriteRecoveryTests(unittest.TestCase):
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
            uow.execute("INSERT INTO user_xiuxian VALUES ('seller',500000,100)")
            uow.executemany(
                "INSERT INTO back(user_id,goods_id,goods_num) VALUES(?,?,?)",
                (("seller", 1001, 3), ("seller", 1002, 2)),
            )
            apply_trade_xianshi_listing(uow)
            apply_trade_xianshi_plan_listing(uow)
        self.repository = XianshiPlanListingSqlRepository(self.database, ids=SequenceIds())
        self.plan = [
            {"goods_id": 1001, "name": "法器", "goods_type": "装备", "price": 600000, "quantity": 2},
            {"goods_id": 1002, "name": "灵草", "goods_type": "药材", "price": 800000, "quantity": 1},
        ]

    def tearDown(self) -> None:
        self.temp_dir.cleanup()

    def _execute(self, sql: str) -> None:
        with DatabaseUnitOfWork(self.database) as uow:
            uow.execute(sql)

    def _rows(self, sql: str):
        with DatabaseUnitOfWork(self.database, read_only=True) as uow:
            return uow.query_all(sql)

    def _list(self, operation_id: str = "plan-1"):
        return self.repository.list_plan(operation_id, "seller", self.plan, stamina_cost=30)

    def _assert_zero_write(self) -> None:
        self.assertEqual(
            self._rows("SELECT stone,user_stamina FROM user_xiuxian"),
            [{"stone": 500000, "user_stamina": 100}],
        )
        self.assertEqual(
            self._rows("SELECT goods_id,goods_num FROM back ORDER BY goods_id"),
            [{"goods_id": 1001, "goods_num": 3}, {"goods_id": 1002, "goods_num": 2}],
        )
        self.assertEqual(self._rows("SELECT * FROM xianshi_item"), [])
        self.assertEqual(self._rows("SELECT * FROM xianshi_plan_listing_operations"), [])

    def test_known_write_rejections_roll_back_and_recover(self) -> None:
        cases = (
            (
                "stamina_rewrite",
                "CREATE TRIGGER fault AFTER UPDATE OF user_stamina ON user_xiuxian "
                "BEGIN UPDATE user_xiuxian SET user_stamina=OLD.user_stamina WHERE user_id=NEW.user_id; END",
                "xianshi_plan_listing_stamina_write_rejected",
                "seller",
            ),
            (
                "stone_rewrite",
                "CREATE TRIGGER fault AFTER UPDATE OF stone ON user_xiuxian "
                "BEGIN UPDATE user_xiuxian SET stone=OLD.stone WHERE user_id=NEW.user_id; END",
                "xianshi_plan_listing_stone_write_rejected",
                "seller",
            ),
            (
                "stock_rewrite",
                "CREATE TRIGGER fault AFTER UPDATE OF goods_num ON back "
                "BEGIN UPDATE back SET goods_num=OLD.goods_num WHERE user_id=NEW.user_id AND goods_id=NEW.goods_id; END",
                "xianshi_plan_listing_stock_write_rejected",
                "seller:1001",
            ),
            (
                "item_insert_ignore",
                "CREATE TRIGGER fault BEFORE INSERT ON xianshi_item BEGIN SELECT RAISE(IGNORE); END",
                "xianshi_plan_listing_item_write_rejected",
                "1000000000001",
            ),
            (
                "item_insert_rewrite",
                "CREATE TRIGGER fault AFTER INSERT ON xianshi_item "
                "BEGIN UPDATE xianshi_item SET quantity=0 WHERE id=NEW.id; END",
                "xianshi_plan_listing_item_write_rejected",
                "1000000000001",
            ),
            (
                "receipt_insert_ignore",
                "CREATE TRIGGER fault BEFORE INSERT ON xianshi_plan_listing_operations BEGIN SELECT RAISE(IGNORE); END",
                "xianshi_plan_listing_receipt_write_rejected",
                "plan-1",
            ),
            (
                "receipt_insert_rewrite",
                "CREATE TRIGGER fault AFTER INSERT ON xianshi_plan_listing_operations "
                "BEGIN UPDATE xianshi_plan_listing_operations SET listed_quantity=0 WHERE operation_id=NEW.operation_id; END",
                "xianshi_plan_listing_receipt_write_rejected",
                "plan-1",
            ),
            (
                "receipt_rewrites_stock",
                "CREATE TRIGGER fault AFTER INSERT ON xianshi_plan_listing_operations "
                "BEGIN UPDATE back SET goods_num=99 WHERE user_id='seller' AND goods_id=1001; END",
                "xianshi_plan_listing_stock_write_rejected",
                "seller:1001",
            ),
        )
        for name, trigger, reason, subject in cases:
            with self.subTest(name=name):
                self._execute(trigger)
                rejected = self._list()
                self.assertEqual(
                    (rejected.status, rejected.reason, rejected.subject),
                    ("state_changed", reason, subject),
                )
                self._assert_zero_write()
                self._execute("DROP TRIGGER fault")
                applied = self._list()
                replay = self._list()
                self.assertEqual((applied.status, replay.status), ("listed", "duplicate"))
                self.assertEqual(self._rows("PRAGMA integrity_check"), [{"integrity_check": "ok"}])
                self.tearDown()
                self.setUp()

    def test_abort_remains_original_exception_and_rolls_back(self) -> None:
        self._execute(
            "CREATE TRIGGER fault BEFORE INSERT ON xianshi_item "
            "BEGIN SELECT RAISE(ABORT,'plan listing failed'); END"
        )
        with self.assertRaisesRegex(sqlite3.IntegrityError, "plan listing failed"):
            self._list()
        self._assert_zero_write()

    def test_operation_conflict_keeps_specific_reason(self) -> None:
        self.assertEqual(self._list().status, "listed")
        changed = [dict(entry) for entry in self.plan]
        changed[0]["price"] += 1
        conflict = self.repository.list_plan("plan-1", "seller", changed, stamina_cost=30)
        self.assertEqual(
            (conflict.status, conflict.reason, conflict.subject),
            ("state_changed", "operation_conflict", "plan-1"),
        )


if __name__ == "__main__":
    unittest.main()
