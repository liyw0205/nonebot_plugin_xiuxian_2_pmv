from __future__ import annotations

import tempfile
import unittest
from pathlib import Path

import nonebot

nonebot.init()

from nonebot_plugin_xiuxian_2.xiuxian.xiuxian_back.breakthrough_rate_item_service import (
    BreakthroughRateItemService,
)
from tests.test_db_backend import db_backend


class BreakthroughRateItemServiceTests(unittest.TestCase):
    def setUp(self) -> None:
        self.temp_dir = tempfile.TemporaryDirectory()
        self.database = Path(self.temp_dir.name) / "breakthrough-rate-item.sqlite3"
        with db_backend.transaction(self.database) as conn:
            conn.execute(
                "CREATE TABLE user_xiuxian "
                "(user_id TEXT PRIMARY KEY, level_up_rate INTEGER)"
            )
            conn.execute(
                "CREATE TABLE back "
                "(user_id TEXT, goods_id INTEGER, goods_num INTEGER, bind_num INTEGER, "
                "day_num INTEGER, all_num INTEGER, UNIQUE(user_id, goods_id))"
            )
            conn.execute(
                "INSERT INTO user_xiuxian VALUES (%s, %s)", ("user", 12)
            )
            conn.execute(
                "INSERT INTO back VALUES (%s, %s, %s, %s, %s, %s)",
                ("user", 15151, 3, 2, 1, 4),
            )
        self.service = BreakthroughRateItemService(self.database)

    def tearDown(self) -> None:
        self.temp_dir.cleanup()

    def state(self):
        with db_backend.connection(self.database) as conn:
            rate = conn.execute(
                "SELECT level_up_rate FROM user_xiuxian WHERE user_id=%s", ("user",)
            ).fetchone()[0]
            item = conn.execute(
                "SELECT goods_num, bind_num, day_num, all_num FROM back "
                "WHERE user_id=%s AND goods_id=%s",
                ("user", 15151),
            ).fetchone()
            operation_count = (
                conn.execute(
                    "SELECT COUNT(*) FROM breakthrough_rate_item_operations"
                ).fetchone()[0]
                if conn.execute(
                    "SELECT 1 FROM sqlite_master WHERE type='table' AND name=%s",
                    ("breakthrough_rate_item_operations",),
                ).fetchone()
                else 0
            )
        return int(rate), tuple(map(int, item)), int(operation_count)

    def test_use_consumes_elixir_and_increases_rate_atomically(self) -> None:
        result = self.service.apply("rate-1", "user", 15151, 2, 10)

        self.assertEqual(result.status, "applied")
        self.assertEqual(self.state(), (22, (1, 0, 3, 6), 1))

    def test_duplicate_does_not_consume_or_increase_twice(self) -> None:
        first = self.service.apply("rate-repeat", "user", 15151, 1, 5)
        second = self.service.apply("rate-repeat", "user", 15151, 3, 99)

        self.assertEqual((first.status, second.status), ("applied", "duplicate"))
        self.assertEqual((second.quantity, second.rate_gain), (1, 5))
        self.assertEqual(self.state(), (17, (2, 1, 2, 5), 1))

    def test_insufficient_elixir_leaves_rate_unchanged(self) -> None:
        result = self.service.apply("rate-poor", "user", 15151, 4, 20)

        self.assertEqual(result.status, "item_insufficient")
        self.assertEqual(self.state(), (12, (3, 2, 1, 4), 0))

    def test_operation_failure_rolls_back_elixir_and_rate(self) -> None:
        with db_backend.transaction(self.database) as conn:
            self.service._ensure_operations(conn)
            conn.execute(
                "CREATE TRIGGER fail_rate_item BEFORE INSERT ON "
                "breakthrough_rate_item_operations "
                "BEGIN SELECT RAISE(ABORT, 'operation failed'); END"
            )

        with self.assertRaises(db_backend.IntegrityError):
            self.service.apply("rate-fail", "user", 15151, 1, 5)

        self.assertEqual(self.state(), (12, (3, 2, 1, 4), 0))


if __name__ == "__main__":
    unittest.main()
