from __future__ import annotations

import tempfile
import unittest
from pathlib import Path

import nonebot

nonebot.init()

from nonebot_plugin_xiuxian_2.xiuxian.xiuxian_back.unbind_item_service import (
    UnbindItemService,
)
from tests.test_db_backend import db_backend


class UnbindItemServiceTests(unittest.TestCase):
    def setUp(self) -> None:
        self.temp_dir = tempfile.TemporaryDirectory()
        self.database = Path(self.temp_dir.name) / "unbind-item.sqlite3"
        with db_backend.transaction(self.database) as conn:
            conn.execute(
                "CREATE TABLE back (user_id TEXT, goods_id INTEGER, goods_num INTEGER, "
                "bind_num INTEGER, UNIQUE(user_id, goods_id))"
            )
            conn.execute("INSERT INTO back VALUES (%s, %s, %s, %s)", ("user", 20019, 3, 2))
            conn.execute("INSERT INTO back VALUES (%s, %s, %s, %s)", ("user", 9001, 5, 4))
        self.service = UnbindItemService(self.database)

    def tearDown(self) -> None:
        self.temp_dir.cleanup()

    def state(self):
        with db_backend.connection(self.database) as conn:
            charm = conn.execute(
                "SELECT goods_num, bind_num FROM back WHERE user_id=%s AND goods_id=%s",
                ("user", 20019),
            ).fetchone()
            target = conn.execute(
                "SELECT goods_num, bind_num FROM back WHERE user_id=%s AND goods_id=%s",
                ("user", 9001),
            ).fetchone()
            count = (
                conn.execute("SELECT COUNT(*) FROM unbind_item_operations").fetchone()[0]
                if conn.execute(
                    "SELECT 1 FROM sqlite_master WHERE type='table' AND name=%s",
                    ("unbind_item_operations",),
                ).fetchone()
                else 0
            )
        return tuple(map(int, charm)), tuple(map(int, target)), int(count)

    def test_unbind_consumes_charms_and_reduces_bound_quantity_atomically(self) -> None:
        result = self.service.apply("unbind-1", "user", 20019, 9001, 2)
        self.assertEqual((result.status, result.quantity), ("applied", 2))
        self.assertEqual(self.state(), ((1, 0), (5, 2), 1))

    def test_quantity_is_capped_by_available_charms_and_bound_items(self) -> None:
        result = self.service.apply("unbind-cap", "user", 20019, 9001, 10)
        self.assertEqual(result.quantity, 3)
        self.assertEqual(self.state(), ((0, 0), (5, 1), 1))

    def test_duplicate_does_not_consume_or_unbind_twice(self) -> None:
        first = self.service.apply("unbind-repeat", "user", 20019, 9001, 1)
        second = self.service.apply("unbind-repeat", "user", 20019, 9001, 3)
        self.assertEqual((first.status, second.status), ("applied", "duplicate"))
        self.assertEqual(second.quantity, 1)
        self.assertEqual(self.state(), ((2, 1), (5, 3), 1))

    def test_unbound_target_does_not_consume_charm(self) -> None:
        with db_backend.transaction(self.database) as conn:
            conn.execute(
                "UPDATE back SET bind_num=0 WHERE user_id=%s AND goods_id=%s",
                ("user", 9001),
            )
        result = self.service.apply("unbind-none", "user", 20019, 9001, 1)
        self.assertEqual(result.status, "not_bound")
        self.assertEqual(self.state(), ((3, 2), (5, 0), 0))

    def test_operation_failure_rolls_back_charm_and_target(self) -> None:
        with db_backend.transaction(self.database) as conn:
            self.service._ensure_operations(conn)
            conn.execute(
                "CREATE TRIGGER fail_unbind BEFORE INSERT ON unbind_item_operations "
                "BEGIN SELECT RAISE(ABORT, 'operation failed'); END"
            )
        with self.assertRaises(db_backend.IntegrityError):
            self.service.apply("unbind-fail", "user", 20019, 9001, 1)
        self.assertEqual(self.state(), ((3, 2), (5, 4), 0))


if __name__ == "__main__":
    unittest.main()
