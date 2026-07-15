from __future__ import annotations

import tempfile
import unittest
from datetime import datetime
from pathlib import Path

import nonebot

nonebot.init()

from nonebot_plugin_xiuxian_2.xiuxian.xiuxian_base.transaction_service import (
    BreakthroughService,
)
from tests.test_db_backend import db_backend


class DirectBreakthroughServiceTests(unittest.TestCase):
    def setUp(self) -> None:
        self.temp_dir = tempfile.TemporaryDirectory()
        self.database = Path(self.temp_dir.name) / "breakthrough.sqlite3"
        with db_backend.transaction(self.database) as conn:
            conn.execute(
                """
                CREATE TABLE user_xiuxian (
                    user_id TEXT PRIMARY KEY,
                    level TEXT,
                    exp INTEGER,
                    hp INTEGER,
                    mp INTEGER,
                    atk INTEGER,
                    power INTEGER,
                    level_up_rate INTEGER,
                    level_up_cd TIMESTAMP
                )
                """
            )
            conn.execute(
                "INSERT INTO user_xiuxian VALUES "
                "(%s, %s, %s, %s, %s, %s, %s, %s, %s)",
                ("user-1", "筑基境初期", 10000, 5000, 10000, 1000, 10000, 5, None),
            )
        self.service = BreakthroughService(self.database)
        self.occurred_at = datetime(2026, 7, 10, 12, 0, 0)

    def tearDown(self) -> None:
        self.temp_dir.cleanup()

    def row(self):
        with db_backend.connection(self.database) as conn:
            return tuple(
                conn.execute(
                    "SELECT level, exp, hp, mp, atk, power, level_up_rate, "
                    "level_up_cd FROM user_xiuxian WHERE user_id=%s",
                    ("user-1",),
                ).fetchone()
            )

    def test_failure_updates_penalty_rate_and_cooldown_atomically(self) -> None:
        result = self.service.apply_failure(
            "failure-1",
            "user-1",
            "筑基境初期",
            10000,
            5000,
            10000,
            5,
            1000,
            4500,
            9000,
            7,
            occurred_at=self.occurred_at,
        )
        self.assertEqual(result.status, "applied")
        row = self.row()
        self.assertEqual(row[:7], ("筑基境初期", 9000, 4500, 9000, 1000, 10000, 7))
        self.assertIsNotNone(row[7])

    def test_success_updates_level_power_attributes_and_cooldown_atomically(self) -> None:
        result = self.service.apply_success(
            "success-1",
            "user-1",
            "筑基境初期",
            "筑基境中期",
            10000,
            5000,
            10000,
            5,
            1.5,
            2.0,
            occurred_at=self.occurred_at,
        )
        self.assertEqual(result.status, "applied")
        row = self.row()
        self.assertEqual(row[:7], ("筑基境中期", 10000, 5000, 10000, 1000, 30000, 0))
        self.assertIsNotNone(row[7])

    def test_duplicate_operation_is_not_applied_twice(self) -> None:
        args = (
            "failure-repeat",
            "user-1",
            "筑基境初期",
            10000,
            5000,
            10000,
            5,
            1000,
            4500,
            9000,
            7,
        )
        self.service.apply_failure(*args, occurred_at=self.occurred_at)
        result = self.service.apply_failure(*args, occurred_at=self.occurred_at)
        self.assertEqual(result.status, "duplicate")
        self.assertEqual(self.row()[1], 9000)

    def test_stale_state_is_rejected(self) -> None:
        result = self.service.apply_failure(
            "stale",
            "user-1",
            "筑基境初期",
            9999,
            5000,
            10000,
            5,
            1000,
            4500,
            9000,
            7,
        )
        self.assertEqual(result.status, "state_changed")
        self.assertEqual(self.row()[:7], ("筑基境初期", 10000, 5000, 10000, 1000, 10000, 5))

    def test_operation_failure_rolls_back_character_state(self) -> None:
        with db_backend.transaction(self.database) as conn:
            self.service._ensure_operations(conn)
            conn.execute(
                """
                CREATE TRIGGER fail_operation
                BEFORE INSERT ON direct_breakthrough_operations
                BEGIN SELECT RAISE(ABORT, 'operation failed'); END
                """
            )
        with self.assertRaises(db_backend.IntegrityError):
            self.service.apply_failure(
                "failure-rollback",
                "user-1",
                "筑基境初期",
                10000,
                5000,
                10000,
                5,
                1000,
                4500,
                9000,
                7,
            )
        self.assertEqual(self.row()[:7], ("筑基境初期", 10000, 5000, 10000, 1000, 10000, 5))


if __name__ == "__main__":
    unittest.main()
