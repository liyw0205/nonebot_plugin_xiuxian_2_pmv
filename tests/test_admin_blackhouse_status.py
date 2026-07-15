from __future__ import annotations

import tempfile
import unittest
from pathlib import Path

import nonebot

nonebot.init()

from nonebot_plugin_xiuxian_2.xiuxian.xiuxian_admin.transaction_service import (
    AdminBlackhouseStatusService,
)
from tests.test_db_backend import db_backend


class AdminBlackhouseStatusTests(unittest.TestCase):
    def setUp(self) -> None:
        self.temp = tempfile.TemporaryDirectory()
        self.database = Path(self.temp.name) / "game.db"
        with db_backend.transaction(self.database) as conn:
            conn.execute(
                "CREATE TABLE user_xiuxian("
                "user_id TEXT PRIMARY KEY,is_ban INTEGER)"
            )
            conn.executemany(
                "INSERT INTO user_xiuxian VALUES(%s,%s)",
                (("free", 0), ("banned", 1)),
            )
        self.service = AdminBlackhouseStatusService(self.database)

    def tearDown(self) -> None:
        self.temp.cleanup()

    def banned(self, user_id):
        with db_backend.connection(self.database) as conn:
            row = conn.execute(
                "SELECT is_ban FROM user_xiuxian WHERE user_id=%s",
                (str(user_id),),
            ).fetchone()
            return bool(row[0]) if row else None

    def set_banned(
        self,
        operation="blackhouse",
        user_id="free",
        expected_banned=False,
        banned=True,
        **changes,
    ):
        values = dict(
            operation_id=operation,
            operator_id="admin",
            user_id=user_id,
            expected_banned=expected_banned,
            banned=banned,
        )
        values.update(changes)
        return self.service.set_banned(**values)

    def test_ban_and_unban_are_atomic_and_idempotent(self) -> None:
        banned = self.set_banned("ban")
        duplicate = self.set_banned("ban", expected_banned=True)
        unbanned = self.set_banned(
            "unban", user_id="free", expected_banned=True, banned=False
        )

        self.assertEqual(
            (banned.status, duplicate.status, unbanned.status),
            ("changed", "duplicate", "changed"),
        )
        self.assertTrue(banned.changed)
        self.assertFalse(self.banned("free"))
        with db_backend.connection(self.database) as conn:
            self.assertEqual(
                2,
                conn.execute(
                    "SELECT COUNT(*) FROM admin_blackhouse_status_operations"
                ).fetchone()[0],
            )

    def test_unchanged_result_is_recorded_and_replayed(self) -> None:
        first = self.set_banned(
            "already", user_id="banned", expected_banned=True, banned=True
        )
        self.set_banned(
            "reverse", user_id="banned", expected_banned=True, banned=False
        )
        duplicate = self.set_banned(
            "already", user_id="banned", expected_banned=False, banned=True
        )

        self.assertEqual((first.status, duplicate.status), ("unchanged", "duplicate"))
        self.assertFalse(first.changed)
        self.assertFalse(duplicate.changed)
        self.assertTrue(duplicate.final_banned)
        self.assertFalse(self.banned("banned"))

    def test_state_and_operation_conflicts_are_rejected(self) -> None:
        stale = self.set_banned("stale", expected_banned=True)
        self.assertEqual(stale.status, "state_changed")

        self.assertEqual(self.set_banned("conflict").status, "changed")
        conflict = self.set_banned(
            "conflict", expected_banned=True, banned=False
        )
        self.assertEqual(conflict.status, "operation_conflict")

    def test_missing_user_and_operation_failure_leave_no_partial_state(self) -> None:
        missing = self.set_banned(
            "missing", user_id="missing", expected_banned=None
        )
        self.assertEqual(missing.status, "user_missing")

        with db_backend.transaction(self.database) as conn:
            self.service._ensure_schema(conn)
            conn.execute(
                "CREATE TRIGGER fail_blackhouse_operation BEFORE INSERT ON "
                "admin_blackhouse_status_operations "
                "BEGIN SELECT RAISE(ABORT,'failed'); END"
            )
        with self.assertRaises(db_backend.IntegrityError):
            self.set_banned("failed")

        self.assertFalse(self.banned("free"))
        with db_backend.connection(self.database) as conn:
            self.assertEqual(
                0,
                conn.execute(
                    "SELECT COUNT(*) FROM admin_blackhouse_status_operations"
                ).fetchone()[0],
            )

    def test_production_entries_use_status_service(self) -> None:
        source = (
            Path(__file__).parents[1]
            / "nonebot_plugin_xiuxian_2/xiuxian/xiuxian_admin/__init__.py"
        ).read_text(encoding="utf-8")
        self.assertGreaterEqual(
            source.count("admin_blackhouse_status_service.set_banned("), 2
        )
        self.assertNotIn("sql_message.ban_user(", source)
        self.assertNotIn("sql_message.unban_user(", source)


if __name__ == "__main__":
    unittest.main()
