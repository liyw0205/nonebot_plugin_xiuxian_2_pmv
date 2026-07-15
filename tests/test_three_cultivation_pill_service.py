from __future__ import annotations

import tempfile
import unittest
from pathlib import Path

import nonebot

nonebot.init()

from nonebot_plugin_xiuxian_2.xiuxian.xiuxian_back.transaction_service import (
    ThreeCultivationPillService,
)
from tests.test_db_backend import db_backend


class ThreeCultivationPillServiceTests(unittest.TestCase):
    def setUp(self) -> None:
        self.temp_dir = tempfile.TemporaryDirectory()
        self.database = Path(self.temp_dir.name) / "three-cultivation-pill.sqlite3"
        with db_backend.transaction(self.database) as conn:
            conn.execute(
                "CREATE TABLE user_xiuxian "
                "(user_id TEXT PRIMARY KEY, exp INTEGER, hp INTEGER, mp INTEGER, "
                "atk INTEGER, power INTEGER)"
            )
            conn.execute(
                "CREATE TABLE back "
                "(user_id TEXT, goods_id INTEGER, goods_num INTEGER, bind_num INTEGER, "
                "UNIQUE(user_id, goods_id))"
            )
            conn.execute(
                "INSERT INTO user_xiuxian VALUES (%s, %s, %s, %s, %s, %s)",
                ("user", 1000, 200, 600, 100, 1500),
            )
            conn.execute(
                "INSERT INTO back VALUES (%s, %s, %s, %s)",
                ("user", 20022, 3, 2),
            )
        self.service = ThreeCultivationPillService(self.database)

    def tearDown(self) -> None:
        self.temp_dir.cleanup()

    def state(self):
        with db_backend.connection(self.database) as conn:
            user = conn.execute(
                "SELECT exp, hp, mp, atk, power FROM user_xiuxian WHERE user_id=%s",
                ("user",),
            ).fetchone()
            item = conn.execute(
                "SELECT goods_num, bind_num FROM back WHERE user_id=%s AND goods_id=%s",
                ("user", 20022),
            ).fetchone()
            operation_count = (
                conn.execute(
                    "SELECT COUNT(*) FROM three_cultivation_pill_operations"
                ).fetchone()[0]
                if conn.execute(
                    "SELECT 1 FROM sqlite_master WHERE type='table' AND name=%s",
                    ("three_cultivation_pill_operations",),
                ).fetchone()
                else 0
            )
        return tuple(map(int, user)), tuple(map(int, item)), int(operation_count)

    def test_use_caps_exp_and_updates_recovery_and_power_atomically(self) -> None:
        result = self.service.apply(
            "pill-1",
            "user",
            20022,
            2,
            900,
            max_exp=1500,
            power_multiplier=1.5,
        )

        self.assertEqual(result.status, "applied")
        self.assertEqual(result.exp_gain, 500)
        self.assertEqual((result.hp_after, result.mp_after), (300, 650))
        self.assertEqual(self.state(), ((1500, 300, 650, 100, 2250), (1, 0), 1))

    def test_duplicate_uses_recorded_roll_and_does_not_apply_twice(self) -> None:
        first = self.service.apply(
            "pill-repeat", "user", 20022, 1, 300,
            max_exp=2000, power_multiplier=1.5,
        )
        second = self.service.apply(
            "pill-repeat", "user", 20022, 3, 9999,
            max_exp=9999, power_multiplier=9,
        )

        self.assertEqual((first.status, second.status), ("applied", "duplicate"))
        self.assertEqual((second.quantity, second.requested_exp), (1, 300))
        self.assertEqual(self.state(), ((1300, 300, 650, 100, 1950), (2, 1), 1))

    def test_at_cap_still_consumes_pill_without_granting_exp(self) -> None:
        result = self.service.apply(
            "pill-cap", "user", 20022, 1, 300,
            max_exp=1000, power_multiplier=1.5,
        )

        self.assertEqual(result.exp_gain, 0)
        self.assertEqual(self.state(), ((1000, 300, 650, 100, 1500), (2, 1), 1))

    def test_recovery_does_not_reduce_attributes_above_current_caps(self) -> None:
        with db_backend.transaction(self.database) as conn:
            conn.execute(
                "UPDATE user_xiuxian SET hp=%s, mp=%s WHERE user_id=%s",
                (800, 1500, "user"),
            )

        result = self.service.apply(
            "pill-over-cap", "user", 20022, 1, 0,
            max_exp=1000, power_multiplier=1.5,
        )

        self.assertEqual((result.hp_after, result.mp_after), (800, 1500))
        self.assertEqual(self.state(), ((1000, 800, 1500, 100, 1500), (2, 1), 1))

    def test_insufficient_item_leaves_character_unchanged(self) -> None:
        result = self.service.apply(
            "pill-poor", "user", 20022, 4, 300,
            max_exp=2000, power_multiplier=1.5,
        )

        self.assertEqual(result.status, "item_insufficient")
        self.assertEqual(self.state(), ((1000, 200, 600, 100, 1500), (3, 2), 0))

    def test_operation_failure_rolls_back_item_and_character(self) -> None:
        with db_backend.transaction(self.database) as conn:
            self.service._ensure_operations(conn)
            conn.execute(
                "CREATE TRIGGER fail_three_pill BEFORE INSERT ON "
                "three_cultivation_pill_operations "
                "BEGIN SELECT RAISE(ABORT, 'operation failed'); END"
            )

        with self.assertRaises(db_backend.IntegrityError):
            self.service.apply(
                "pill-fail", "user", 20022, 1, 300,
                max_exp=2000, power_multiplier=1.5,
            )

        self.assertEqual(self.state(), ((1000, 200, 600, 100, 1500), (3, 2), 0))


if __name__ == "__main__":
    unittest.main()
