from __future__ import annotations

import tempfile
import unittest
from pathlib import Path

import nonebot

nonebot.init()

from nonebot_plugin_xiuxian_2.xiuxian.xiuxian_trade.repository import TradeRepository
from tests.test_db_backend import db_backend


class GuishiTakeStoredItemTests(unittest.TestCase):
    def setUp(self) -> None:
        self.temp_dir = tempfile.TemporaryDirectory()
        root = Path(self.temp_dir.name)
        self.game_database = root / "game.sqlite3"
        self.trade_database = root / "trade.sqlite3"
        with db_backend.transaction(self.game_database) as conn:
            conn.execute(
                """
                CREATE TABLE back (
                    user_id TEXT,
                    goods_id INTEGER,
                    goods_name TEXT,
                    goods_type TEXT,
                    goods_num INTEGER,
                    create_time TEXT,
                    update_time TEXT,
                    bind_num INTEGER DEFAULT 0,
                    UNIQUE (user_id, goods_id)
                )
                """
            )
        with db_backend.transaction(self.trade_database) as conn:
            conn.execute(
                """
                CREATE TABLE guishi_info (
                    user_id TEXT PRIMARY KEY,
                    stored_stone INTEGER DEFAULT 0,
                    items TEXT DEFAULT '{}'
                )
                """
            )
            conn.execute(
                "INSERT INTO guishi_info (user_id, stored_stone, items) VALUES (%s, %s, %s)",
                ("user", 0, '{"1001": 3}'),
            )
        self.repository = TradeRepository(self.game_database, max_goods_num=99)

    def tearDown(self) -> None:
        self.temp_dir.cleanup()

    def stored_quantity(self) -> int:
        row = db_backend.query_one(
            self.trade_database,
            "SELECT items FROM guishi_info WHERE user_id = %s",
            "user",
        )
        if row is None:
            return 0
        items = row["items"] or "{}"
        import json

        return int(json.loads(items).get("1001", 0))

    def inventory(self) -> tuple[int, int] | None:
        row = db_backend.query_one(
            self.game_database,
            "SELECT goods_num, bind_num FROM back WHERE user_id = %s AND goods_id = %s",
            ("user", 1001),
        )
        if row is None:
            return None
        return int(row["goods_num"]), int(row["bind_num"])

    def operation_count(self) -> int:
        with db_backend.connection(self.game_database) as conn:
            exists = conn.execute(
                "SELECT 1 FROM sqlite_master WHERE type='table' AND name=%s",
                ("guishi_take_item_operations",),
            ).fetchone()
            if exists is None:
                return 0
            return int(
                conn.execute(
                    "SELECT COUNT(*) FROM guishi_take_item_operations"
                ).fetchone()[0]
            )

    def take(self, operation_id: str = "take-1"):
        return self.repository.take_guishi_stored_item(
            operation_id,
            self.trade_database,
            "user",
            1001,
            "测试法器",
            "装备",
        )

    def test_take_moves_stored_item_into_bound_inventory_atomically(self) -> None:
        result = self.take()

        self.assertEqual(result.status, "taken")
        self.assertEqual(result.quantity, 3)
        self.assertEqual(self.stored_quantity(), 0)
        self.assertEqual(self.inventory(), (3, 3))
        self.assertEqual(self.operation_count(), 1)

    def test_duplicate_operation_does_not_add_inventory_twice(self) -> None:
        first = self.take("take-repeat")
        second = self.take("take-repeat")

        self.assertEqual((first.status, second.status), ("taken", "duplicate"))
        self.assertEqual(second.quantity, 3)
        self.assertEqual(self.stored_quantity(), 0)
        self.assertEqual(self.inventory(), (3, 3))
        self.assertEqual(self.operation_count(), 1)

    def test_missing_item_leaves_state_unchanged(self) -> None:
        with db_backend.transaction(self.trade_database) as conn:
            conn.execute(
                "UPDATE guishi_info SET items=%s WHERE user_id=%s",
                ('{}', "user"),
            )

        result = self.take("take-missing")

        self.assertEqual(result.status, "item_missing")
        self.assertEqual(self.stored_quantity(), 0)
        self.assertIsNone(self.inventory())
        self.assertEqual(self.operation_count(), 0)

    def test_inventory_full_keeps_stored_item_available(self) -> None:
        with db_backend.transaction(self.game_database) as conn:
            conn.execute(
                "INSERT INTO back VALUES (%s, %s, %s, %s, %s, NULL, NULL, %s)",
                ("user", 1001, "测试法器", "装备", 99, 0),
            )

        result = self.take("take-full")

        self.assertEqual(result.status, "inventory_full")
        self.assertEqual(result.quantity, 3)
        self.assertEqual(self.stored_quantity(), 3)
        self.assertEqual(self.inventory(), (99, 0))
        self.assertEqual(self.operation_count(), 0)

    def test_operation_insert_failure_rolls_back_storage_and_inventory(self) -> None:
        self.take("init-schema")
        with db_backend.transaction(self.trade_database) as conn:
            conn.execute(
                "UPDATE guishi_info SET items=%s WHERE user_id=%s",
                ('{"1001": 3}', "user"),
            )
        with db_backend.transaction(self.game_database) as conn:
            conn.execute("DELETE FROM back WHERE user_id=%s AND goods_id=%s", ("user", 1001))
            conn.execute(
                "CREATE TRIGGER fail_take_operation BEFORE INSERT "
                "ON guishi_take_item_operations "
                "WHEN NEW.operation_id='take-fail' "
                "BEGIN SELECT RAISE(ABORT, 'operation failed'); END"
            )

        with self.assertRaises(db_backend.IntegrityError):
            self.take("take-fail")

        self.assertEqual(self.stored_quantity(), 3)
        self.assertIsNone(self.inventory())
        self.assertEqual(self.operation_count(), 1)


if __name__ == "__main__":
    unittest.main()
