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


class ContinuousBreakthroughServiceTests(unittest.TestCase):
    def setUp(self) -> None:
        self.temp_dir = tempfile.TemporaryDirectory()
        self.database = Path(self.temp_dir.name) / "continuous.sqlite3"
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
        self.occurred_at = datetime(2026, 7, 11, 12, 0, 0)

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

    def apply(self, operation_id="continuous-1", **overrides):
        values = {
            "user_id": "user-1",
            "expected_level": "筑基境初期",
            "expected_exp": 10000,
            "expected_hp": 5000,
            "expected_mp": 10000,
            "expected_rate": 5,
            "final_level": "筑基境初期",
            "final_exp": 7000,
            "final_hp": 3500,
            "final_mp": 7000,
            "final_rate": 11,
            "attempts": 3,
            "fail_count": 3,
            "exp_loss": 3000,
            "occurred_at": self.occurred_at,
        }
        values.update(overrides)
        return self.service.apply_continuous(operation_id, **values)

    def test_failed_attempts_commit_final_state_once(self) -> None:
        result = self.apply()
        self.assertEqual(result.status, "applied")
        row = self.row()
        self.assertEqual(row[:7], ("筑基境初期", 7000, 3500, 7000, 1000, 10000, 11))
        self.assertIsNotNone(row[7])

    def test_success_uses_final_exp_for_power_and_attributes(self) -> None:
        result = self.apply(
            "continuous-success",
            final_level="筑基境中期",
            final_exp=8000,
            final_hp=4000,
            final_mp=8000,
            final_rate=0,
            attempts=3,
            fail_count=2,
            exp_loss=2000,
            root_rate=1.5,
            level_spend=2.0,
        )
        self.assertEqual(result.status, "applied")
        row = self.row()
        self.assertEqual(row[:7], ("筑基境中期", 8000, 4000, 8000, 800, 24000, 0))

    def test_duplicate_does_not_apply_final_state_twice(self) -> None:
        self.apply("continuous-repeat")
        result = self.apply("continuous-repeat")
        self.assertEqual(result.status, "duplicate")
        self.assertEqual(self.row()[1], 7000)

    def test_stale_initial_state_is_rejected(self) -> None:
        result = self.apply("continuous-stale", expected_exp=9999)
        self.assertEqual(result.status, "state_changed")
        self.assertEqual(self.row()[:7], ("筑基境初期", 10000, 5000, 10000, 1000, 10000, 5))

    def test_operation_failure_rolls_back_final_state(self) -> None:
        with db_backend.transaction(self.database) as conn:
            self.service._ensure_continuous_operations(conn)
            conn.execute(
                """
                CREATE TRIGGER fail_operation
                BEFORE INSERT ON continuous_breakthrough_operations
                BEGIN SELECT RAISE(ABORT, 'operation failed'); END
                """
            )
        with self.assertRaises(db_backend.IntegrityError):
            self.apply("continuous-rollback")
        self.assertEqual(self.row()[:7], ("筑基境初期", 10000, 5000, 10000, 1000, 10000, 5))


if __name__ == "__main__":
    unittest.main()
