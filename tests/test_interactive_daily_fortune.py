from __future__ import annotations

import tempfile
import unittest
from datetime import date, timedelta
from pathlib import Path

import nonebot

nonebot.init()

from nonebot_plugin_xiuxian_2.xiuxian.xiuxian_Interactive.transaction_service import (
    InteractiveDailyFortuneService,
)
from tests.test_db_backend import db_backend


class InteractiveDailyFortuneTests(unittest.TestCase):
    def setUp(self) -> None:
        self.temp = tempfile.TemporaryDirectory()
        self.database = Path(self.temp.name) / "game.db"
        with db_backend.transaction(self.database) as conn:
            conn.execute("CREATE TABLE user_xiuxian(user_id TEXT PRIMARY KEY)")
            conn.executemany(
                "INSERT INTO user_xiuxian VALUES(%s)",
                (("1",), ("2",)),
            )
        self.service = InteractiveDailyFortuneService(self.database)
        self.day = date(2026, 7, 14)

    def tearDown(self) -> None:
        self.temp.cleanup()

    @staticmethod
    def fortune(name="吉"):
        return {
            "type": name,
            "description": f"{name}签文",
            "stars": "*****",
        }

    def resolve(self, operation="fortune", user_id="1", factory=None, **changes):
        values = dict(
            operation_id=operation,
            user_id=user_id,
            business_date=self.day,
            create_fortune=factory or (lambda: self.fortune()),
        )
        values.update(changes)
        return self.service.resolve(**values)

    def test_first_result_is_fixed_and_another_event_does_not_regenerate(self) -> None:
        first = self.resolve("first")
        existing = self.resolve(
            "another", factory=lambda: self.fail("fortune regenerated")
        )

        self.assertEqual((first.status, existing.status), ("generated", "existing"))
        self.assertEqual(first.fortune, existing.fortune)
        with db_backend.connection(self.database) as conn:
            self.assertEqual(
                1,
                conn.execute(
                    "SELECT COUNT(*) FROM interactive_daily_fortunes"
                ).fetchone()[0],
            )
            self.assertEqual(
                2,
                conn.execute(
                    "SELECT COUNT(*) FROM interactive_daily_fortune_operations"
                ).fetchone()[0],
            )

    def test_operation_replay_does_not_invoke_factory_and_conflicts(self) -> None:
        first = self.resolve("replay", factory=lambda: self.fortune("大吉"))
        duplicate = self.resolve(
            "replay", factory=lambda: self.fail("duplicate regenerated")
        )
        conflict = self.resolve(
            "replay",
            user_id="2",
            factory=lambda: self.fail("conflict regenerated"),
        )

        self.assertEqual(
            (first.status, duplicate.status, conflict.status),
            ("generated", "duplicate", "operation_conflict"),
        )
        self.assertEqual(first.fortune, duplicate.fortune)

    def test_next_business_date_gets_a_new_fortune(self) -> None:
        first = self.resolve("today", factory=lambda: self.fortune("吉"))
        tomorrow = self.resolve(
            "tomorrow",
            business_date=self.day + timedelta(days=1),
            factory=lambda: self.fortune("小吉"),
        )

        self.assertEqual(first.fortune_type, "吉")
        self.assertEqual(tomorrow.fortune_type, "小吉")

    def test_missing_user_and_operation_failure_leave_no_fortune(self) -> None:
        missing = self.resolve("missing", user_id="missing")
        self.assertEqual(missing.status, "user_missing")

        with db_backend.transaction(self.database) as conn:
            self.service._ensure_schema(conn)
            conn.execute(
                "CREATE TRIGGER fail_fortune_operation BEFORE INSERT ON "
                "interactive_daily_fortune_operations "
                "BEGIN SELECT RAISE(ABORT,'failed'); END"
            )
        with self.assertRaises(db_backend.IntegrityError):
            self.resolve("failed")

        with db_backend.connection(self.database) as conn:
            self.assertEqual(
                0,
                conn.execute(
                    "SELECT COUNT(*) FROM interactive_daily_fortunes"
                ).fetchone()[0],
            )

    def test_invalid_factory_result_rolls_back(self) -> None:
        with self.assertRaisesRegex(ValueError, "fields are required"):
            self.resolve("invalid", factory=lambda: {"type": "吉"})
        with db_backend.connection(self.database) as conn:
            if conn.table_exists("interactive_daily_fortunes"):
                self.assertEqual(
                    0,
                    conn.execute(
                        "SELECT COUNT(*) FROM interactive_daily_fortunes"
                    ).fetchone()[0],
                )

    def test_cleanup_and_production_entry(self) -> None:
        self.resolve("old", business_date=self.day - timedelta(days=31))
        self.resolve("current")
        self.assertEqual(
            2,
            self.service.cleanup_before(self.day - timedelta(days=30)),
        )

        source = (
            Path(__file__).parents[1]
            / "nonebot_plugin_xiuxian_2/xiuxian/xiuxian_Interactive/__init__.py"
        ).read_text(encoding="utf-8")
        self.assertIn("interactive_daily_fortune_service.resolve(", source)
        self.assertIn("interactive_daily_fortune_service.cleanup_before(", source)
        for old_call in (
            "load_fortune_data(",
            "save_fortune_data(",
            "get_user_fortune(",
            "load_json_file(",
            "save_json_file(",
        ):
            self.assertNotIn(old_call, source)


if __name__ == "__main__":
    unittest.main()
