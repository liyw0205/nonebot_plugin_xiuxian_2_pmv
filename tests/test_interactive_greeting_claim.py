from __future__ import annotations

import tempfile
import unittest
from datetime import date, timedelta
from pathlib import Path

import nonebot

nonebot.init()

from nonebot_plugin_xiuxian_2.xiuxian.xiuxian_Interactive.greeting_claim_service import (
    InteractiveGreetingClaimService,
)
from tests.test_db_backend import db_backend


class InteractiveGreetingClaimTests(unittest.TestCase):
    def setUp(self) -> None:
        self.temp = tempfile.TemporaryDirectory()
        self.database = Path(self.temp.name) / "game.db"
        with db_backend.transaction(self.database) as conn:
            conn.execute("CREATE TABLE user_xiuxian(user_id TEXT PRIMARY KEY)")
            conn.executemany(
                "INSERT INTO user_xiuxian VALUES(%s)",
                (("1",), ("2",), ("3",)),
            )
        self.service = InteractiveGreetingClaimService(self.database)
        self.day = date(2026, 7, 14)

    def tearDown(self) -> None:
        self.temp.cleanup()

    def claim(self, operation="greeting", user_id="1", kind="morning", **changes):
        values = dict(
            operation_id=operation,
            user_id=user_id,
            kind=kind,
            business_date=self.day,
        )
        values.update(changes)
        return self.service.claim(**values)

    def test_daily_positions_are_unique_and_kinds_are_independent(self) -> None:
        first = self.claim("morning-1", "1")
        second = self.claim("morning-2", "2")
        night = self.claim("night-1", "1", "night")

        self.assertEqual(
            (first.position, second.position, night.position),
            (1, 2, 1),
        )
        self.assertTrue(first.succeeded)
        with db_backend.connection(self.database) as conn:
            self.assertEqual(
                3,
                conn.execute(
                    "SELECT COUNT(*) FROM interactive_greeting_claims"
                ).fetchone()[0],
            )

    def test_event_replay_and_another_event_keep_first_outcomes(self) -> None:
        first = self.claim("first")
        duplicate = self.claim("first")
        already = self.claim("another")
        already_duplicate = self.claim("another")

        self.assertEqual(
            (first.status, duplicate.status, already.status, already_duplicate.status),
            ("claimed", "duplicate", "already_claimed", "duplicate"),
        )
        self.assertEqual(
            (first.claimed, duplicate.claimed, already.claimed, already_duplicate.claimed),
            (True, True, False, False),
        )
        self.assertEqual(
            {first.position, duplicate.position, already.position, already_duplicate.position},
            {1},
        )

    def test_next_date_can_claim_again_and_payload_conflicts(self) -> None:
        first = self.claim("dated")
        conflict = self.claim("dated", kind="night")
        next_day = self.claim(
            "next-day", business_date=self.day + timedelta(days=1)
        )

        self.assertEqual(first.status, "claimed")
        self.assertEqual(conflict.status, "operation_conflict")
        self.assertEqual((next_day.status, next_day.position), ("claimed", 1))

    def test_missing_user_and_operation_failure_leave_no_claim(self) -> None:
        missing = self.claim("missing", user_id="missing")
        self.assertEqual(missing.status, "user_missing")

        with db_backend.transaction(self.database) as conn:
            self.service._ensure_schema(conn)
            conn.execute(
                "CREATE TRIGGER fail_greeting_operation BEFORE INSERT ON "
                "interactive_greeting_operations "
                "BEGIN SELECT RAISE(ABORT,'failed'); END"
            )
        with self.assertRaises(db_backend.IntegrityError):
            self.claim("failed")

        with db_backend.connection(self.database) as conn:
            self.assertEqual(
                0,
                conn.execute(
                    "SELECT COUNT(*) FROM interactive_greeting_claims"
                ).fetchone()[0],
            )

    def test_cleanup_only_removes_old_business_dates(self) -> None:
        self.claim("old", business_date=self.day - timedelta(days=31))
        self.claim("current")

        removed = self.service.cleanup_before(self.day - timedelta(days=30))

        self.assertEqual(removed, 2)
        with db_backend.connection(self.database) as conn:
            self.assertEqual(
                1,
                conn.execute(
                    "SELECT COUNT(*) FROM interactive_greeting_claims"
                ).fetchone()[0],
            )
            self.assertEqual(
                1,
                conn.execute(
                    "SELECT COUNT(*) FROM interactive_greeting_operations"
                ).fetchone()[0],
            )

    def test_production_entries_use_greeting_service(self) -> None:
        source = (
            Path(__file__).parents[1]
            / "nonebot_plugin_xiuxian_2/xiuxian/xiuxian_Interactive/__init__.py"
        ).read_text(encoding="utf-8")
        self.assertGreaterEqual(
            source.count("interactive_greeting_claim_service.claim("), 2
        )
        self.assertIn("interactive_greeting_claim_service.cleanup_before(", source)
        for old_call in (
            "has_user_triggered(",
            "mark_user_triggered(",
            "get_current_count(",
            "save_count_data(",
        ):
            self.assertNotIn(old_call, source)


if __name__ == "__main__":
    unittest.main()
