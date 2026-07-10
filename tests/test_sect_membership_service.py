from __future__ import annotations

import tempfile
import unittest
from pathlib import Path

import nonebot

nonebot.init()

from nonebot_plugin_xiuxian_2.xiuxian.xiuxian_sect.membership_service import (
    SectMembershipService,
)
from tests.test_db_backend import db_backend


class SectMembershipServiceTests(unittest.TestCase):
    def setUp(self) -> None:
        self.temp_dir = tempfile.TemporaryDirectory()
        self.database = Path(self.temp_dir.name) / "sect.sqlite3"
        with db_backend.transaction(self.database) as conn:
            conn.execute(
                """
                CREATE TABLE user_xiuxian (
                    user_id TEXT PRIMARY KEY,
                    sect_id INTEGER,
                    sect_position INTEGER,
                    user_name TEXT
                )
                """
            )
            conn.execute(
                """
                CREATE TABLE sects (
                    sect_id INTEGER PRIMARY KEY,
                    sect_name TEXT NOT NULL,
                    sect_owner TEXT
                )
                """
            )
            conn.execute("INSERT INTO sects VALUES (%s, %s, %s)", (1, "青云宗", "owner"))
            conn.execute("INSERT INTO sects VALUES (%s, %s, %s)", (2, "天音寺", "other"))
            conn.execute("INSERT INTO user_xiuxian VALUES (%s, %s, %s, %s)", ("owner", 1, 0, "旧宗主"))
            conn.execute("INSERT INTO user_xiuxian VALUES (%s, %s, %s, %s)", ("member", 1, 3, "新宗主"))
            conn.execute("INSERT INTO user_xiuxian VALUES (%s, %s, %s, %s)", ("other", 2, 0, "外宗主"))
        self.service = SectMembershipService(self.database)

    def tearDown(self) -> None:
        self.temp_dir.cleanup()

    def scalar(self, sql, params=()):
        with db_backend.connection(self.database) as conn:
            row = conn.execute(sql, params).fetchone()
            return row[0] if row else None

    def test_transfer_updates_owner_and_both_positions_atomically(self) -> None:
        result = self.service.transfer_owner("transfer-1", "owner", "member")

        self.assertEqual(result.status, "transferred")
        self.assertEqual(result.sect_name, "青云宗")
        self.assertEqual(self.scalar("SELECT sect_owner FROM sects WHERE sect_id=%s", (1,)), "member")
        self.assertEqual(self.scalar("SELECT sect_position FROM user_xiuxian WHERE user_id=%s", ("owner",)), 1)
        self.assertEqual(self.scalar("SELECT sect_position FROM user_xiuxian WHERE user_id=%s", ("member",)), 0)

    def test_repeated_operation_is_idempotent(self) -> None:
        first = self.service.transfer_owner("transfer-repeat", "owner", "member")
        second = self.service.transfer_owner("transfer-repeat", "owner", "member")

        self.assertEqual(first.status, "transferred")
        self.assertEqual(second.status, "duplicate")
        self.assertEqual(second.target_name, "新宗主")
        self.assertEqual(second.sect_name, "青云宗")
        self.assertEqual(self.scalar("SELECT COUNT(*) FROM sect_operations"), 1)

    def test_missing_target_does_not_change_owner(self) -> None:
        result = self.service.transfer_owner("missing", "owner", "missing")

        self.assertEqual(result.status, "target_missing")
        self.assertEqual(self.scalar("SELECT sect_owner FROM sects WHERE sect_id=%s", (1,)), "owner")

    def test_target_from_another_sect_is_rejected(self) -> None:
        result = self.service.transfer_owner("outsider", "owner", "other")

        self.assertEqual(result.status, "target_not_member")
        self.assertEqual(self.scalar("SELECT sect_owner FROM sects WHERE sect_id=%s", (1,)), "owner")

    def test_non_owner_is_rejected_using_current_database_state(self) -> None:
        result = self.service.transfer_owner("not-owner", "member", "owner")

        self.assertEqual(result.status, "not_owner")
        self.assertEqual(self.scalar("SELECT sect_owner FROM sects WHERE sect_id=%s", (1,)), "owner")

    def test_self_transfer_is_rejected(self) -> None:
        result = self.service.transfer_owner("self", "owner", "owner")

        self.assertEqual(result.status, "self_transfer")

    def test_database_failure_rolls_back_all_owner_changes(self) -> None:
        with db_backend.transaction(self.database) as conn:
            conn.execute(
                """
                CREATE TRIGGER fail_owner BEFORE UPDATE ON sects
                BEGIN SELECT RAISE(ABORT, 'owner failed'); END
                """
            )

        with self.assertRaises(db_backend.IntegrityError):
            self.service.transfer_owner("transfer-fail", "owner", "member")

        self.assertEqual(self.scalar("SELECT sect_owner FROM sects WHERE sect_id=%s", (1,)), "owner")
        self.assertEqual(self.scalar("SELECT sect_position FROM user_xiuxian WHERE user_id=%s", ("owner",)), 0)
        self.assertEqual(self.scalar("SELECT sect_position FROM user_xiuxian WHERE user_id=%s", ("member",)), 3)
        with db_backend.connection(self.database) as conn:
            table = conn.execute(
                "SELECT name FROM sqlite_master WHERE type='table' AND name=%s",
                ("sect_operations",),
            ).fetchone()
        self.assertIsNone(table)


if __name__ == "__main__":
    unittest.main()
