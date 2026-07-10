from __future__ import annotations

import tempfile
import unittest
from datetime import datetime
from pathlib import Path

import nonebot

nonebot.init()

from nonebot_plugin_xiuxian_2.xiuxian.xiuxian_base.breakthrough_service import (
    BreakthroughService,
)
from tests.test_db_backend import db_backend


class TribulationBreakthroughServiceTests(unittest.TestCase):
    def setUp(self) -> None:
        self.temp_dir = tempfile.TemporaryDirectory()
        self.database = Path(self.temp_dir.name) / "tribulation.sqlite3"
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
                    goods_name TEXT,
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
                "INSERT INTO back "
                "(user_id, goods_id, goods_name, goods_type, goods_num, bind_num) "
                "VALUES (%s, %s, %s, %s, %s, %s)",
                ("user-1", 1999, "渡厄丹", "丹药", 2, 1),
            )
        self.service = BreakthroughService(self.database)
        self.occurred_at = datetime(2026, 7, 10, 12, 0, 0)

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

    def test_failure_consumes_pill_and_updates_rate_atomically(self) -> None:
        result = self.service.apply_tribulation_failure(
            "failure-1", "user-1", "筑基境初期", 10000, 5000, 10000, 5, 7, 1999,
            occurred_at=self.occurred_at,
        )
        self.assertEqual(result.status, "applied")
        user, item = self.state()
        self.assertEqual(user, ("筑基境初期", 10000, 5000, 10000, 1000, 10000, 7))
        self.assertEqual(item, (1, 1, 1, 1))

    def test_success_consumes_pill_and_updates_character_atomically(self) -> None:
        result = self.service.apply_tribulation_success(
            "success-1", "user-1", "筑基境初期", "筑基境中期",
            10000, 5000, 10000, 5, 1.5, 2.0, 1999,
            occurred_at=self.occurred_at,
        )
        self.assertEqual(result.status, "applied")
        user, item = self.state()
        self.assertEqual(user, ("筑基境中期", 10000, 5000, 10000, 1000, 30000, 0))
        self.assertEqual(item, (1, 1, 1, 1))

    def test_duplicate_does_not_consume_second_pill(self) -> None:
        args = (
            "repeat", "user-1", "筑基境初期", 10000, 5000, 10000, 5, 7, 1999
        )
        self.service.apply_tribulation_failure(*args)
        result = self.service.apply_tribulation_failure(*args)
        self.assertEqual(result.status, "duplicate")
        self.assertEqual(self.state()[1][0], 1)

    def test_missing_pill_does_not_change_character(self) -> None:
        with db_backend.transaction(self.database) as conn:
            conn.execute(
                "UPDATE back SET goods_num=0 WHERE user_id=%s AND goods_id=%s",
                ("user-1", 1999),
            )
        result = self.service.apply_tribulation_failure(
            "missing", "user-1", "筑基境初期", 10000, 5000, 10000, 5, 7, 1999
        )
        self.assertEqual(result.status, "item_missing")
        self.assertEqual(self.state()[0][-1], 5)

    def test_operation_failure_rolls_back_item_and_character(self) -> None:
        with db_backend.transaction(self.database) as conn:
            self.service._ensure_tribulation_operations(conn)
            conn.execute(
                """
                CREATE TRIGGER fail_operation
                BEFORE INSERT ON tribulation_breakthrough_operations
                BEGIN SELECT RAISE(ABORT, 'operation failed'); END
                """
            )
        with self.assertRaises(db_backend.IntegrityError):
            self.service.apply_tribulation_failure(
                "rollback", "user-1", "筑基境初期", 10000, 5000, 10000, 5, 7, 1999
            )
        user, item = self.state()
        self.assertEqual(user[-1], 5)
        self.assertEqual(item, (2, 0, 0, 1))

    def test_gold_pill_failure_grants_exp_in_same_transaction(self) -> None:
        result = self.service.apply_tribulation_failure(
            "gold-failure", "user-1", "筑基境初期", 10000, 5000, 10000,
            5, 7, 1999, exp_gain=1000,
        )
        self.assertEqual(result.status, "applied")
        user, item = self.state()
        self.assertEqual(user[1], 11000)
        self.assertEqual(user[-1], 7)
        self.assertEqual(item[0], 1)

    def test_gold_pill_success_uses_post_reward_exp_for_attributes(self) -> None:
        result = self.service.apply_tribulation_success(
            "gold-success", "user-1", "筑基境初期", "筑基境中期",
            10000, 5000, 10000, 5, 1.5, 2.0, 1999, exp_gain=1000,
        )
        self.assertEqual(result.status, "applied")
        user, item = self.state()
        self.assertEqual(user, ("筑基境中期", 11000, 5500, 11000, 1100, 33000, 0))
        self.assertEqual(item[0], 1)


if __name__ == "__main__":
    unittest.main()
