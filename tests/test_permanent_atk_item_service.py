from __future__ import annotations

import tempfile
import unittest
from pathlib import Path

import nonebot

nonebot.init()

from nonebot_plugin_xiuxian_2.xiuxian.xiuxian_back.transaction_service import (
    PermanentAtkItemService,
)
from tests.test_db_backend import db_backend


class PermanentAtkItemServiceTests(unittest.TestCase):
    def setUp(self) -> None:
        self.temp_dir = tempfile.TemporaryDirectory()
        self.database = Path(self.temp_dir.name) / "permanent-atk-item.sqlite3"
        with db_backend.transaction(self.database) as conn:
            conn.execute("CREATE TABLE BuffInfo (user_id TEXT PRIMARY KEY, atk_buff INTEGER)")
            conn.execute(
                "CREATE TABLE back (user_id TEXT, goods_id INTEGER, goods_num INTEGER, "
                "bind_num INTEGER, day_num INTEGER, all_num INTEGER, "
                "UNIQUE(user_id, goods_id))"
            )
            conn.execute("INSERT INTO BuffInfo VALUES (%s, %s)", ("user", 20))
            conn.execute(
                "INSERT INTO back VALUES (%s, %s, %s, %s, %s, %s)",
                ("user", 15002, 3, 2, 0, 1),
            )
        self.service = PermanentAtkItemService(self.database)

    def tearDown(self) -> None:
        self.temp_dir.cleanup()

    def state(self):
        with db_backend.connection(self.database) as conn:
            atk = conn.execute(
                "SELECT atk_buff FROM BuffInfo WHERE user_id=%s", ("user",)
            ).fetchone()[0]
            item = conn.execute(
                "SELECT goods_num, bind_num, day_num, all_num FROM back "
                "WHERE user_id=%s AND goods_id=%s",
                ("user", 15002),
            ).fetchone()
            count = (
                conn.execute("SELECT COUNT(*) FROM permanent_atk_item_operations").fetchone()[0]
                if conn.execute(
                    "SELECT 1 FROM sqlite_master WHERE type='table' AND name=%s",
                    ("permanent_atk_item_operations",),
                ).fetchone()
                else 0
            )
        return int(atk), tuple(map(int, item)), int(count)

    def test_use_updates_attack_and_inventory_atomically(self) -> None:
        result = self.service.apply("atk-1", "user", 15002, 2, 12)
        self.assertEqual(result.status, "applied")
        self.assertEqual(self.state(), (32, (1, 0, 2, 3), 1))

    def test_duplicate_does_not_apply_twice(self) -> None:
        first = self.service.apply("atk-repeat", "user", 15002, 1, 6)
        second = self.service.apply("atk-repeat", "user", 15002, 3, 99)
        self.assertEqual((first.status, second.status), ("applied", "duplicate"))
        self.assertEqual((second.quantity, second.atk_gain), (1, 6))
        self.assertEqual(self.state(), (26, (2, 1, 1, 2), 1))

    def test_insufficient_item_leaves_attack_unchanged(self) -> None:
        result = self.service.apply("atk-poor", "user", 15002, 4, 24)
        self.assertEqual(result.status, "item_insufficient")
        self.assertEqual(self.state(), (20, (3, 2, 0, 1), 0))

    def test_operation_failure_rolls_back_attack_and_inventory(self) -> None:
        with db_backend.transaction(self.database) as conn:
            self.service._ensure_operations(conn)
            conn.execute(
                "CREATE TRIGGER fail_atk_item BEFORE INSERT ON permanent_atk_item_operations "
                "BEGIN SELECT RAISE(ABORT, 'operation failed'); END"
            )
        with self.assertRaises(db_backend.IntegrityError):
            self.service.apply("atk-fail", "user", 15002, 1, 6)
        self.assertEqual(self.state(), (20, (3, 2, 0, 1), 0))


if __name__ == "__main__":
    unittest.main()
