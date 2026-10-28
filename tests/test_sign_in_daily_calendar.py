from __future__ import annotations

import sqlite3
import tempfile
import unittest
from datetime import date, datetime, timezone
from pathlib import Path

from nonebot_plugin_xiuxian_2.features.sign_in.application import SignInApplication
from nonebot_plugin_xiuxian_2.features.sign_in.calendar import (
    render_sign_in_calendar,
    signed_streak,
)
from nonebot_plugin_xiuxian_2.features.sign_in.migrations import apply_sign_in_daily
from nonebot_plugin_xiuxian_2.features.sign_in.repository import SignInRepository
from nonebot_plugin_xiuxian_2.infrastructure.database import DatabaseUnitOfWork, OperationLedger


class FixedClock:
    def now(self):
        return datetime(2026, 10, 15, 8, 0, tzinfo=timezone.utc)


class FixedRandom:
    def randint(self, lower, upper):
        return 1666


def _database(root: Path, *, daily: bool = True, legacy_signed: bool = False) -> Path:
    path = root / "game.db"
    with DatabaseUnitOfWork(path) as uow:
        uow.execute(
            "CREATE TABLE user_xiuxian (user_id TEXT PRIMARY KEY, user_name TEXT, "
            "stone INTEGER, is_sign INTEGER)"
        )
        uow.execute(
            "INSERT INTO user_xiuxian VALUES (?,?,?,?)", ("u1", "道友", 100, 0)
        )
        if legacy_signed:
            uow.execute(
                "INSERT INTO user_xiuxian VALUES (?,?,?,?)", ("u2", "旧道友", 100, 1)
            )
        OperationLedger().ensure_schema(uow)
        SignInRepository().ensure_schema(uow)
        if daily:
            apply_sign_in_daily(uow)
    return path


def _application(database: Path) -> SignInApplication:
    return SignInApplication(
        database,
        clock=FixedClock(),
        random_source=FixedRandom(),
        lower_limit=100,
        upper_limit=100,
    )


class SignInDailyReceiptTests(unittest.TestCase):
    def setUp(self) -> None:
        self.temp_dir = tempfile.TemporaryDirectory()
        self.root = Path(self.temp_dir.name)
        self.database = _database(self.root)
        self.application = _application(self.database)

    def tearDown(self) -> None:
        self.temp_dir.cleanup()

    def test_success_writes_one_daily_receipt(self) -> None:
        result = self.application.claim(user_id="u1", operation_id="sign-1")

        self.assertEqual("applied", result.status)
        with DatabaseUnitOfWork(self.database, read_only=True) as uow:
            user = uow.query_one("SELECT stone,is_sign FROM user_xiuxian WHERE user_id='u1'")
            daily = uow.query_all("SELECT business_date,stone,operation_id FROM sign_in_daily")
        assert user is not None
        self.assertEqual((1766, 1), (int(user["stone"]), int(user["is_sign"])))
        self.assertEqual(
            [("2026-10-15", 1666, "sign-1")],
            [(str(row["business_date"]), int(row["stone"]), str(row["operation_id"])) for row in daily],
        )

    def test_replay_does_not_add_a_second_receipt(self) -> None:
        self.application.claim(user_id="u1", operation_id="sign-1")
        replay = self.application.claim(user_id="u1", operation_id="sign-1")

        self.assertTrue(replay.replayed)
        with DatabaseUnitOfWork(self.database, read_only=True) as uow:
            count = uow.query_one("SELECT COUNT(*) AS total FROM sign_in_daily")
            user = uow.query_one("SELECT stone FROM user_xiuxian WHERE user_id='u1'")
        assert count is not None and user is not None
        self.assertEqual(1, int(count["total"]))
        self.assertEqual(1766, int(user["stone"]))

    def test_same_day_receipt_blocks_a_second_claim_after_flag_reset(self) -> None:
        self.application.claim(user_id="u1", operation_id="sign-1")
        with DatabaseUnitOfWork(self.database, immediate=True) as uow:
            uow.execute("UPDATE user_xiuxian SET is_sign=0 WHERE user_id='u1'")

        second = self.application.claim(user_id="u1", operation_id="sign-2")

        self.assertEqual(("rejected", "already_signed"), (second.status, second.code))
        with DatabaseUnitOfWork(self.database, read_only=True) as uow:
            user = uow.query_one("SELECT stone FROM user_xiuxian WHERE user_id='u1'")
            count = uow.query_one("SELECT COUNT(*) AS total FROM sign_in_daily")
        assert user is not None and count is not None
        self.assertEqual(1766, int(user["stone"]))
        self.assertEqual(1, int(count["total"]))

    def test_delayed_reset_protects_the_same_day_signer(self) -> None:
        self.application.claim(user_id="u1", operation_id="sign-1")

        reset = self.application.reset_daily_flags("2026-10-15")

        self.assertEqual(("applied", 0, 1), (reset.status, reset.reset_count, reset.protected_count))
        with DatabaseUnitOfWork(self.database, read_only=True) as uow:
            user = uow.query_one("SELECT is_sign FROM user_xiuxian WHERE user_id='u1'")
        assert user is not None
        self.assertEqual(1, int(user["is_sign"]))

    def test_reset_still_clears_legacy_rows_without_a_receipt(self) -> None:
        with DatabaseUnitOfWork(self.database, immediate=True) as uow:
            uow.execute(
                "INSERT INTO user_xiuxian VALUES (?,?,?,?)", ("u2", "旧道友", 100, 1)
            )

        reset = self.application.reset_daily_flags("2026-10-15")

        self.assertEqual((1, 0), (reset.reset_count, reset.protected_count))
        with DatabaseUnitOfWork(self.database, read_only=True) as uow:
            user = uow.query_one("SELECT is_sign FROM user_xiuxian WHERE user_id='u2'")
        assert user is not None
        self.assertEqual(0, int(user["is_sign"]))

    def test_reset_replay_keeps_the_first_counts(self) -> None:
        self.application.claim(user_id="u1", operation_id="sign-1")
        first = self.application.reset_daily_flags("2026-10-15")
        replay = self.application.reset_daily_flags("2026-10-15")

        self.assertEqual("duplicate", replay.status)
        self.assertEqual((first.reset_count, first.protected_count), (replay.reset_count, replay.protected_count))

    def test_reply_contains_the_calendar_after_signing(self) -> None:
        reply = self.application.reply(user_id="u1", operation_id="sign-1")

        self.assertIn("签到成功", reply.content)
        self.assertIn("【签到日历 2026-10】", reply.content)
        self.assertIn("15✓", reply.content)
        self.assertIn("本月已签 1 天｜连续 1 天", reply.content)

    def test_reply_contains_the_calendar_when_already_signed(self) -> None:
        self.application.claim(user_id="u1", operation_id="sign-1")
        reply = self.application.reply(user_id="u1", operation_id="sign-2")

        self.assertIn("今日已经签到", reply.content)
        self.assertIn("【签到日历 2026-10】", reply.content)


class SignInDailyCompatibilityTests(unittest.TestCase):
    def setUp(self) -> None:
        self.temp_dir = tempfile.TemporaryDirectory()
        self.root = Path(self.temp_dir.name)

    def tearDown(self) -> None:
        self.temp_dir.cleanup()

    def test_missing_daily_table_keeps_the_legacy_claim_and_reset(self) -> None:
        database = _database(self.root, daily=False)
        application = _application(database)

        result = application.claim(user_id="u1", operation_id="sign-1")
        self.assertEqual("applied", result.status)
        self.assertEqual("", application.calendar_text("u1"))
        reset = application.reset_daily_flags("2026-10-15")

        self.assertEqual((1, 0), (reset.reset_count, reset.protected_count))
        with DatabaseUnitOfWork(database, read_only=True) as uow:
            user = uow.query_one("SELECT is_sign FROM user_xiuxian WHERE user_id='u1'")
            table = uow.query_one(
                "SELECT 1 AS present FROM sqlite_master WHERE type='table' AND name='sign_in_daily'"
            )
        assert user is not None
        self.assertEqual(0, int(user["is_sign"]))
        self.assertIsNone(table)

    def test_missing_database_returns_no_calendar(self) -> None:
        missing = self.root / "missing.db"
        application = _application(missing)

        self.assertEqual("", application.calendar_text("u1"))
        self.assertFalse(missing.exists())

    def test_migration_creates_the_table_and_index(self) -> None:
        database = _database(self.root)

        with sqlite3.connect(database) as connection:
            tables = {row[0] for row in connection.execute("SELECT name FROM sqlite_master")}
            indexes = {
                row[0]
                for row in connection.execute(
                    "SELECT name FROM sqlite_master WHERE type='index' AND name='sign_in_daily_date'"
                )
            }

        self.assertIn("sign_in_daily", tables)
        self.assertEqual({"sign_in_daily_date"}, indexes)


class SignInCalendarRenderingTests(unittest.TestCase):
    def test_streak_ends_today_when_today_is_signed(self) -> None:
        self.assertEqual(3, signed_streak(["2026-10-13", "2026-10-14", "2026-10-15"], "2026-10-15"))

    def test_streak_falls_back_to_yesterday_when_today_is_unsigned(self) -> None:
        self.assertEqual(2, signed_streak(["2026-10-13", "2026-10-14"], "2026-10-15"))
        self.assertEqual(0, signed_streak([], "2026-10-15"))

    def test_calendar_marks_signed_and_today(self) -> None:
        lines = render_sign_in_calendar("2026-10-15", ["2026-10-13", "2026-10-14"])

        self.assertEqual("【签到日历 2026-10】", lines[0])
        self.assertEqual("一  二  三  四  五  六  日", lines[1])
        self.assertIn("13✓", "\n".join(lines))
        self.assertIn("14✓", "\n".join(lines))
        self.assertIn("15·", "\n".join(lines))
        self.assertEqual("本月已签 2 天｜连续 2 天", lines[-1])

    def test_calendar_aligns_the_first_day_of_the_month(self) -> None:
        lines = render_sign_in_calendar("2026-10-15", [])

        # 2026-10-01 is a Thursday, so the first week starts with three blanks.
        self.assertTrue(lines[2].startswith("            "))
        self.assertIn(" 1 ", lines[2])

    def test_calendar_includes_every_week_of_the_month(self) -> None:
        lines = render_sign_in_calendar("2026-10-31", ["2026-10-31"])

        self.assertIn("31✓", "\n".join(lines))
        self.assertEqual("本月已签 1 天｜连续 1 天", lines[-1])


class SignInWiringTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls) -> None:
        root = Path(__file__).resolve().parents[1]
        cls.application_source = (
            root / "nonebot_plugin_xiuxian_2" / "features" / "sign_in" / "application.py"
        ).read_text(encoding="utf-8")
        cls.reset_source = (
            root / "nonebot_plugin_xiuxian_2" / "features" / "sign_in" / "daily_reset_repository.py"
        ).read_text(encoding="utf-8")

    def test_claim_checks_and_records_the_daily_receipt(self) -> None:
        self.assertIn("daily_signed(", self.application_source)
        self.assertIn("record_daily(", self.application_source)
        self.assertIn("business_date", self.application_source)

    def test_reset_excludes_users_with_a_same_day_receipt(self) -> None:
        self.assertIn("sign_in_daily", self.reset_source)
        self.assertIn("NOT EXISTS", self.reset_source)
        self.assertIn("protected_count", self.reset_source)


if __name__ == "__main__":
    unittest.main()
