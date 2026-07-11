from __future__ import annotations

import tempfile
import unittest
from pathlib import Path

import nonebot

nonebot.init()

from nonebot_plugin_xiuxian_2.xiuxian.xiuxian_back.cultivation_item_service import (
    CultivationItemService,
)
from tests.test_db_backend import db_backend


class CultivationItemServiceTests(unittest.TestCase):
    def setUp(self) -> None:
        self.temp_dir = tempfile.TemporaryDirectory()
        self.database = Path(self.temp_dir.name) / "cultivation-item.sqlite3"
        with db_backend.transaction(self.database) as conn:
            conn.execute(
                "CREATE TABLE user_xiuxian "
                "(user_id TEXT PRIMARY KEY, exp INTEGER, hp INTEGER, mp INTEGER, "
                "atk INTEGER, power INTEGER)"
            )
            conn.execute(
                "CREATE TABLE back "
                "(user_id TEXT, goods_id INTEGER, goods_num INTEGER, bind_num INTEGER, "
                "day_num INTEGER DEFAULT 0, all_num INTEGER DEFAULT 0, "
                "UNIQUE(user_id, goods_id))"
            )
            conn.execute(
                "INSERT INTO user_xiuxian VALUES (%s, %s, %s, %s, %s, %s)",
                ("user", 1000, 500, 700, 100, 1500),
            )
            conn.execute(
                "INSERT INTO back (user_id, goods_id, goods_num, bind_num) "
                "VALUES (%s, %s, %s, %s)",
                ("user", 9001, 3, 2),
            )
        self.service = CultivationItemService(self.database)

    def tearDown(self) -> None:
        self.temp_dir.cleanup()

    def state(self):
        with db_backend.connection(self.database) as conn:
            user = conn.execute(
                "SELECT exp, hp, mp, atk, power FROM user_xiuxian WHERE user_id=%s",
                ("user",),
            ).fetchone()
            item = conn.execute(
                "SELECT goods_num, bind_num, day_num, all_num FROM back "
                "WHERE user_id=%s AND goods_id=%s",
                ("user", 9001),
            ).fetchone()
            operation_count = (
                conn.execute("SELECT COUNT(*) FROM cultivation_item_operations").fetchone()[0]
                if conn.execute(
                    "SELECT 1 FROM sqlite_master WHERE type='table' AND name=%s",
                    ("cultivation_item_operations",),
                ).fetchone()
                else 0
            )
        return tuple(map(int, user)), tuple(map(int, item)), int(operation_count)

    def test_use_consumes_items_and_updates_all_attributes_atomically(self) -> None:
        result = self.service.apply(
            "use-1",
            "user",
            9001,
            2,
            200,
            hp_gain=100,
            mp_gain=200,
            atk_gain=20,
            power_multiplier=1.5,
        )

        self.assertEqual(result.status, "applied")
        self.assertEqual(self.state(), ((1200, 600, 900, 120, 1800), (1, 0, 0, 0), 1))

    def test_elixir_usage_updates_tolerance_counters_in_same_transaction(self) -> None:
        result = self.service.apply(
            "use-elixir",
            "user",
            9001,
            2,
            200,
            hp_gain=100,
            mp_gain=200,
            atk_gain=20,
            power_multiplier=1.5,
            track_usage=True,
        )

        self.assertEqual(result.status, "applied")
        self.assertEqual(self.state(), ((1200, 600, 900, 120, 1800), (1, 0, 2, 2), 1))

    def test_duplicate_event_does_not_consume_or_reward_twice(self) -> None:
        first = self.service.apply(
            "use-repeat", "user", 9001, 1, 100,
            hp_gain=50, mp_gain=100, atk_gain=10, power_multiplier=1.5,
        )
        second = self.service.apply(
            "use-repeat", "user", 9001, 3, 999,
            hp_gain=999, mp_gain=999, atk_gain=999, power_multiplier=9,
        )

        self.assertEqual((first.status, second.status), ("applied", "duplicate"))
        self.assertEqual((second.quantity, second.exp_gain), (1, 100))
        self.assertEqual(self.state(), ((1100, 550, 800, 110, 1650), (2, 1, 0, 0), 1))

    def test_insufficient_item_leaves_character_unchanged(self) -> None:
        result = self.service.apply(
            "use-poor", "user", 9001, 4, 400,
            hp_gain=200, mp_gain=400, atk_gain=40, power_multiplier=1.5,
        )

        self.assertEqual(result.status, "item_insufficient")
        self.assertEqual(self.state(), ((1000, 500, 700, 100, 1500), (3, 2, 0, 0), 0))

    def test_database_failure_rolls_back_item_and_character(self) -> None:
        with db_backend.transaction(self.database) as conn:
            self.service._ensure_operations(conn)
            conn.execute(
                "CREATE TRIGGER fail_cultivation_item BEFORE INSERT ON "
                "cultivation_item_operations "
                "BEGIN SELECT RAISE(ABORT, 'operation failed'); END"
            )

        with self.assertRaises(db_backend.IntegrityError):
            self.service.apply(
                "use-fail", "user", 9001, 1, 100,
                hp_gain=50, mp_gain=100, atk_gain=10, power_multiplier=1.5,
            )

        self.assertEqual(self.state(), ((1000, 500, 700, 100, 1500), (3, 2, 0, 0), 0))


if __name__ == "__main__":
    unittest.main()
