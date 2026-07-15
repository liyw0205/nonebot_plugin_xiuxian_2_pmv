from __future__ import annotations

import tempfile
import unittest
from pathlib import Path

import nonebot

nonebot.init()

from nonebot_plugin_xiuxian_2.xiuxian.xiuxian_base.transaction_service import SignInService
from tests.test_db_backend import db_backend


class SignInServiceTests(unittest.TestCase):
    def setUp(self) -> None:
        self.temp_dir = tempfile.TemporaryDirectory()
        self.database = Path(self.temp_dir.name) / "sign.sqlite3"
        with db_backend.transaction(self.database) as conn:
            conn.execute(
                """
                CREATE TABLE user_xiuxian (
                    user_id TEXT PRIMARY KEY,
                    is_sign INTEGER NOT NULL DEFAULT 0,
                    stone INTEGER NOT NULL DEFAULT 0
                )
                """
            )
            conn.execute("INSERT INTO user_xiuxian VALUES (%s, %s, %s)", ("u1", 0, 100))
            conn.execute("INSERT INTO user_xiuxian VALUES (%s, %s, %s)", ("u2", 1, 200))
        self.service = SignInService(self.database, randint=lambda lower, upper: 25)
        with db_backend.transaction(self.database) as conn:
            self.service._ensure_operations(conn)

    def tearDown(self) -> None:
        self.temp_dir.cleanup()

    def scalar(self, sql, params=()):
        with db_backend.connection(self.database) as conn:
            row = conn.execute(sql, params).fetchone()
            return row[0] if row else None

    def test_sign_awards_stone_and_marks_user_in_one_transaction(self) -> None:
        result = self.service.sign("sign-1", "u1", 10, 30)

        self.assertEqual(result.status, "signed")
        self.assertEqual(result.stone, 25)
        self.assertEqual(self.scalar("SELECT stone FROM user_xiuxian WHERE user_id=%s", ("u1",)), 125)
        self.assertEqual(self.scalar("SELECT is_sign FROM user_xiuxian WHERE user_id=%s", ("u1",)), 1)

    def test_repeated_event_does_not_award_twice(self) -> None:
        first = self.service.sign("sign-repeat", "u1", 10, 30)
        second = self.service.sign("sign-repeat", "u1", 10, 30)

        self.assertEqual(first.status, "signed")
        self.assertEqual(second.status, "duplicate")
        self.assertEqual(second.stone, 25)
        self.assertEqual(self.scalar("SELECT stone FROM user_xiuxian WHERE user_id=%s", ("u1",)), 125)

    def test_already_signed_and_missing_user_do_not_create_operation(self) -> None:
        signed = self.service.sign("already", "u2", 10, 30)
        missing = self.service.sign("missing", "missing", 10, 30)

        self.assertEqual(signed.status, "already_signed")
        self.assertEqual(missing.status, "user_missing")
        self.assertEqual(self.scalar("SELECT COUNT(*) FROM sign_in_operations"), 0)

    def test_database_failure_rolls_back_reward_and_sign_state(self) -> None:
        with db_backend.transaction(self.database) as conn:
            conn.execute(
                """
                CREATE TRIGGER fail_sign_operation BEFORE INSERT ON sign_in_operations
                BEGIN SELECT RAISE(ABORT, 'operation failed'); END
                """
            )

        with self.assertRaises(db_backend.IntegrityError):
            self.service.sign("sign-fail", "u1", 10, 30)

        self.assertEqual(self.scalar("SELECT stone FROM user_xiuxian WHERE user_id=%s", ("u1",)), 100)
        self.assertEqual(self.scalar("SELECT is_sign FROM user_xiuxian WHERE user_id=%s", ("u1",)), 0)


if __name__ == "__main__":
    unittest.main()
