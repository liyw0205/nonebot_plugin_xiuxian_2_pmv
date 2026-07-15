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


class ContinuousTribulationServiceTests(unittest.TestCase):
    def setUp(self) -> None:
        self.temp_dir = tempfile.TemporaryDirectory()
        self.database = Path(self.temp_dir.name) / "continuous-tribulation.sqlite3"
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
                """
                CREATE TABLE back (
                    user_id TEXT,
                    goods_id INTEGER,
                    goods_type TEXT,
                    goods_num INTEGER,
                    update_time TIMESTAMP,
                    action_time TIMESTAMP,
                    day_num INTEGER DEFAULT 0,
                    all_num INTEGER DEFAULT 0,
                    bind_num INTEGER DEFAULT 0,
                    PRIMARY KEY (user_id, goods_id)
                )
                """
            )
            conn.execute(
                "INSERT INTO user_xiuxian VALUES "
                "(%s, %s, %s, %s, %s, %s, %s, %s, %s)",
                ("user-1", "筑基境初期", 10000, 5000, 10000, 1000, 10000, 5, None),
            )
            conn.execute(
                "INSERT INTO back (user_id, goods_id, goods_type, goods_num, bind_num) "
                "VALUES (%s, %s, %s, %s, %s)",
                ("user-1", 1999, "丹药", 5, 2),
            )
        self.service = BreakthroughService(self.database)
        self.occurred_at = datetime(2026, 7, 11, 12, 0, 0)

    def tearDown(self) -> None:
        self.temp_dir.cleanup()

    def state(self):
        with db_backend.connection(self.database) as conn:
            user = tuple(
                conn.execute(
                    "SELECT level, exp, hp, mp, atk, power, level_up_rate "
                    "FROM user_xiuxian WHERE user_id=%s",
                    ("user-1",),
                ).fetchone()
            )
            item = tuple(
                conn.execute(
                    "SELECT goods_num, day_num, all_num, bind_num FROM back "
                    "WHERE user_id=%s AND goods_id=%s",
                    ("user-1", 1999),
                ).fetchone()
            )
            return user, item

    def apply(self, operation_id="continuous-dr", **overrides):
        values = {
            "user_id": "user-1",
            "expected_level": "筑基境初期",
            "expected_exp": 10000,
            "expected_hp": 5000,
            "expected_mp": 10000,
            "expected_rate": 5,
            "final_level": "筑基境初期",
            "final_exp": 10000,
            "final_rate": 11,
            "attempts": 3,
            "fail_count": 3,
            "item_id": 1999,
            "item_count": 3,
            "exp_gain": 0,
            "occurred_at": self.occurred_at,
        }
        values.update(overrides)
        return self.service.apply_continuous_tribulation(operation_id, **values)

    def test_failures_consume_all_used_pills_and_update_rate_atomically(self) -> None:
        result = self.apply()
        self.assertEqual(result.status, "applied")
        user, item = self.state()
        self.assertEqual(user, ("筑基境初期", 10000, 5000, 10000, 1000, 10000, 11))
        self.assertEqual(item, (2, 3, 3, 2))

    def test_success_consumes_attempt_pills_and_updates_character(self) -> None:
        result = self.apply(
            "continuous-dr-success",
            final_level="筑基境中期",
            final_rate=0,
            attempts=3,
            fail_count=2,
            root_rate=1.5,
            level_spend=2.0,
        )
        self.assertEqual(result.status, "applied")
        user, item = self.state()
        self.assertEqual(user, ("筑基境中期", 10000, 5000, 10000, 1000, 30000, 0))
        self.assertEqual(item[0], 2)

    def test_duplicate_does_not_consume_pills_twice(self) -> None:
        self.apply("continuous-dr-repeat")
        result = self.apply("continuous-dr-repeat")
        self.assertEqual(result.status, "duplicate")
        self.assertEqual(self.state()[1][0], 2)

    def test_insufficient_pills_does_not_change_character(self) -> None:
        result = self.apply("continuous-dr-missing", item_count=6)
        self.assertEqual(result.status, "item_missing")
        user, item = self.state()
        self.assertEqual(user[-1], 5)
        self.assertEqual(item[0], 5)

    def test_operation_failure_rolls_back_pills_and_character(self) -> None:
        with db_backend.transaction(self.database) as conn:
            self.service._ensure_continuous_tribulation_operations(conn)
            conn.execute(
                """
                CREATE TRIGGER fail_operation
                BEFORE INSERT ON continuous_tribulation_operations
                BEGIN SELECT RAISE(ABORT, 'operation failed'); END
                """
            )
        with self.assertRaises(db_backend.IntegrityError):
            self.apply("continuous-dr-rollback")
        user, item = self.state()
        self.assertEqual(user[-1], 5)
        self.assertEqual(item, (5, 0, 0, 2))

    def test_gold_pills_commit_cumulative_exp_gain_with_consumption(self) -> None:
        result = self.apply(
            "continuous-gold-failure",
            final_exp=13310,
            final_rate=11,
            item_id=1999,
            item_count=3,
            exp_gain=3310,
        )
        self.assertEqual(result.status, "applied")
        user, item = self.state()
        self.assertEqual(user[1], 13310)
        self.assertEqual(user[-1], 11)
        self.assertEqual(item[0], 2)

    def test_gold_success_uses_accumulated_exp_for_power_and_attributes(self) -> None:
        result = self.apply(
            "continuous-gold-success",
            final_level="筑基境中期",
            final_exp=13310,
            final_rate=0,
            attempts=3,
            fail_count=2,
            item_count=3,
            exp_gain=3310,
            root_rate=1.5,
            level_spend=2.0,
        )
        self.assertEqual(result.status, "applied")
        user, item = self.state()
        self.assertEqual(user, ("筑基境中期", 13310, 6655, 13310, 1331, 39930, 0))
        self.assertEqual(item[0], 2)


if __name__ == "__main__":
    unittest.main()
