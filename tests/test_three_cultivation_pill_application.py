from __future__ import annotations

import sqlite3
import tempfile
import unittest
from pathlib import Path

from nonebot_plugin_xiuxian_2.features.back.migrations import apply_three_cultivation_pill
from nonebot_plugin_xiuxian_2.features.back.three_cultivation_pill_application import ThreeCultivationPillApplication
from nonebot_plugin_xiuxian_2.infrastructure.database import DatabaseUnitOfWork
from tests.test_db_backend import db_backend


class ThreeCultivationPillApplicationTests(unittest.TestCase):
    def setUp(self) -> None:
        self.temp_dir = tempfile.TemporaryDirectory()
        self.database = Path(self.temp_dir.name) / "three-cultivation-pill.sqlite3"
        with db_backend.transaction(self.database) as conn:
            conn.execute(
                "CREATE TABLE user_xiuxian (user_id TEXT PRIMARY KEY, exp INTEGER, hp INTEGER, "
                "mp INTEGER, atk INTEGER, power INTEGER)"
            )
            conn.execute(
                "CREATE TABLE back (user_id TEXT, goods_id INTEGER, goods_num INTEGER, bind_num INTEGER, "
                "UNIQUE(user_id,goods_id))"
            )
            conn.execute(
                "INSERT INTO user_xiuxian VALUES (%s,%s,%s,%s,%s,%s)",
                ("user", 1000, 200, 600, 100, 1500),
            )
            conn.execute("INSERT INTO back VALUES (%s,%s,%s,%s)", ("user", 20022, 3, 2))
        with DatabaseUnitOfWork(self.database) as uow:
            apply_three_cultivation_pill(uow)
        self.application = ThreeCultivationPillApplication(self.database)

    def tearDown(self) -> None:
        self.temp_dir.cleanup()

    def state(self):
        with db_backend.connection(self.database) as conn:
            user = conn.execute(
                "SELECT exp,hp,mp,atk,power FROM user_xiuxian WHERE user_id=%s", ("user",)
            ).fetchone()
            item = conn.execute(
                "SELECT goods_num,bind_num FROM back WHERE user_id=%s AND goods_id=%s",
                ("user", 20022),
            ).fetchone()
            count = conn.execute("SELECT COUNT(*) FROM three_cultivation_pill_operations").fetchone()[0]
        return tuple(map(int, user)), tuple(map(int, item)), int(count)

    def apply(self, operation_id="pill-1", quantity=2, requested_exp=900, **overrides):
        args = {
            "operation_id": operation_id,
            "user_id": "user",
            "item_id": 20022,
            "quantity": quantity,
            "requested_exp": requested_exp,
            "max_exp": 1500,
            "power_multiplier": 1.5,
        }
        args.update(overrides)
        return self.application.apply(**args)

    def test_caps_exp_updates_recovery_power_and_consumes_items(self) -> None:
        result = self.apply()
        self.assertEqual((result.status, result.exp_gain, result.hp_after, result.mp_after), ("applied", 500, 300, 650))
        self.assertEqual(self.state(), ((1500, 300, 650, 100, 2250), (1, 0), 1))

    def test_duplicate_replays_recorded_result_without_mutation(self) -> None:
        first = self.apply(operation_id="pill-repeat", quantity=1, requested_exp=300, max_exp=2000)
        second = self.apply(operation_id="pill-repeat", quantity=3, requested_exp=9999, max_exp=9999, power_multiplier=9)
        self.assertEqual((first.status, second.status), ("applied", "duplicate"))
        self.assertEqual((second.quantity, second.requested_exp), (1, 300))
        self.assertEqual(self.state(), ((1300, 300, 650, 100, 1950), (2, 1), 1))

    def test_at_cap_still_consumes_without_exp_gain(self) -> None:
        result = self.apply(operation_id="pill-cap", quantity=1, requested_exp=300, max_exp=1000)
        self.assertEqual(result.exp_gain, 0)
        self.assertEqual(self.state(), ((1000, 300, 650, 100, 1500), (2, 1), 1))

    def test_recovery_does_not_reduce_values_above_caps(self) -> None:
        with db_backend.transaction(self.database) as conn:
            conn.execute("UPDATE user_xiuxian SET hp=%s,mp=%s WHERE user_id=%s", (800, 1500, "user"))
        result = self.apply(operation_id="pill-over-cap", quantity=1, requested_exp=0, max_exp=1000)
        self.assertEqual((result.hp_after, result.mp_after), (800, 1500))
        self.assertEqual(self.state(), ((1000, 800, 1500, 100, 1500), (2, 1), 1))

    def test_trigger_failure_rolls_back_item_and_character(self) -> None:
        with db_backend.transaction(self.database) as conn:
            conn.execute(
                "CREATE TRIGGER fail_three_pill BEFORE INSERT ON three_cultivation_pill_operations "
                "BEGIN SELECT RAISE(ABORT, 'operation failed'); END"
            )
        with self.assertRaises(db_backend.IntegrityError):
            self.apply(operation_id="pill-fail", quantity=1, requested_exp=300, max_exp=2000)
        self.assertEqual(self.state(), ((1000, 200, 600, 100, 1500), (3, 2), 0))

    def test_missing_schema_is_not_created_at_request_time(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            database = Path(directory) / "empty.sqlite3"
            with DatabaseUnitOfWork(database) as uow:
                uow.execute("CREATE TABLE user_xiuxian(user_id TEXT PRIMARY KEY, exp INTEGER, hp INTEGER, mp INTEGER, power INTEGER)")
                uow.execute("CREATE TABLE back(user_id TEXT, goods_id INTEGER, goods_num INTEGER)")
            with self.assertRaises(sqlite3.OperationalError):
                ThreeCultivationPillApplication(database).apply(
                    "missing-schema", "user", 20022, 1, 300, max_exp=2000, power_multiplier=1.5
                )


if __name__ == "__main__":
    unittest.main()
