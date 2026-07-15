from __future__ import annotations

import tempfile
import unittest
from pathlib import Path

import nonebot

nonebot.init()

from nonebot_plugin_xiuxian_2.xiuxian.xiuxian_back.transaction_service import (
    RecoveryItemService,
)
from tests.test_db_backend import db_backend


class RecoveryItemServiceTests(unittest.TestCase):
    def setUp(self) -> None:
        self.temp_dir = tempfile.TemporaryDirectory()
        self.database = Path(self.temp_dir.name) / "recovery-item.sqlite3"
        with db_backend.transaction(self.database) as conn:
            conn.execute(
                "CREATE TABLE user_xiuxian "
                "(user_id TEXT PRIMARY KEY, exp INTEGER, hp INTEGER, mp INTEGER, "
                "atk INTEGER, user_stamina INTEGER)"
            )
            conn.execute(
                "CREATE TABLE back "
                "(user_id TEXT, goods_id INTEGER, goods_num INTEGER, bind_num INTEGER, "
                "day_num INTEGER, all_num INTEGER, UNIQUE(user_id, goods_id))"
            )
            conn.execute(
                "INSERT INTO user_xiuxian VALUES (%s, %s, %s, %s, %s, %s)",
                ("user", 1000, 100, 200, 80, 120),
            )
            conn.execute(
                "INSERT INTO back VALUES (%s, %s, %s, %s, %s, %s)",
                ("user", 15001, 3, 2, 0, 4),
            )
        self.service = RecoveryItemService(self.database)

    def tearDown(self) -> None:
        self.temp_dir.cleanup()

    def state(self):
        with db_backend.connection(self.database) as conn:
            user = conn.execute(
                "SELECT exp, hp, mp, atk, user_stamina FROM user_xiuxian "
                "WHERE user_id=%s",
                ("user",),
            ).fetchone()
            item = conn.execute(
                "SELECT goods_num, bind_num, day_num, all_num FROM back "
                "WHERE user_id=%s AND goods_id=%s",
                ("user", 15001),
            ).fetchone()
            operation_count = (
                conn.execute("SELECT COUNT(*) FROM recovery_item_operations").fetchone()[0]
                if conn.execute(
                    "SELECT 1 FROM sqlite_master WHERE type='table' AND name=%s",
                    ("recovery_item_operations",),
                ).fetchone()
                else 0
            )
        return tuple(map(int, user)), tuple(map(int, item)), int(operation_count)

    def test_hp_mp_recovery_consumes_item_and_updates_state_atomically(self) -> None:
        result = self.service.apply(
            "recovery-1", "user", 15001, 2,
            mode="hp_mp", hp_gain=200, mp_gain=500,
        )

        self.assertEqual(result.status, "applied")
        self.assertEqual((result.hp_after, result.mp_after), (300, 700))
        self.assertEqual(self.state(), ((1000, 300, 700, 80, 120), (1, 0, 2, 6), 1))

    def test_full_recovery_resets_hp_mp_and_atk_from_exp(self) -> None:
        result = self.service.apply("recovery-full", "user", 15001, 1, mode="full")

        self.assertEqual(result.status, "applied")
        self.assertEqual(self.state(), ((1000, 500, 1000, 100, 120), (2, 1, 1, 5), 1))

    def test_stamina_recovery_updates_only_stamina(self) -> None:
        result = self.service.apply(
            "recovery-stamina", "user", 15001, 1,
            mode="stamina", stamina_gain=300, max_stamina=500,
        )

        self.assertEqual((result.stamina_before, result.stamina_after), (120, 420))
        self.assertEqual(self.state(), ((1000, 100, 200, 80, 420), (2, 1, 1, 5), 1))

    def test_duplicate_does_not_consume_or_recover_twice(self) -> None:
        first = self.service.apply(
            "recovery-repeat", "user", 15001, 1,
            mode="hp_mp", hp_gain=200, mp_gain=500,
        )
        second = self.service.apply(
            "recovery-repeat", "user", 15001, 3,
            mode="hp_mp", hp_gain=900, mp_gain=900,
        )

        self.assertEqual((first.status, second.status), ("applied", "duplicate"))
        self.assertEqual((second.quantity, second.hp_after, second.mp_after), (1, 300, 700))
        self.assertEqual(self.state(), ((1000, 300, 700, 80, 120), (2, 1, 1, 5), 1))

    def test_insufficient_item_leaves_state_unchanged(self) -> None:
        result = self.service.apply(
            "recovery-poor", "user", 15001, 4,
            mode="hp_mp", hp_gain=200, mp_gain=500,
        )

        self.assertEqual(result.status, "item_insufficient")
        self.assertEqual(self.state(), ((1000, 100, 200, 80, 120), (3, 2, 0, 4), 0))

    def test_operation_failure_rolls_back_item_and_state(self) -> None:
        with db_backend.transaction(self.database) as conn:
            self.service._ensure_operations(conn)
            conn.execute(
                "CREATE TRIGGER fail_recovery BEFORE INSERT ON recovery_item_operations "
                "BEGIN SELECT RAISE(ABORT, 'operation failed'); END"
            )

        with self.assertRaises(db_backend.IntegrityError):
            self.service.apply(
                "recovery-fail", "user", 15001, 1,
                mode="hp_mp", hp_gain=200, mp_gain=500,
            )

        self.assertEqual(self.state(), ((1000, 100, 200, 80, 120), (3, 2, 0, 4), 0))


if __name__ == "__main__":
    unittest.main()
