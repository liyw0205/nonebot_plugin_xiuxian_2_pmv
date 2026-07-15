from __future__ import annotations

import tempfile
import unittest
from pathlib import Path

import nonebot

nonebot.init()

from nonebot_plugin_xiuxian_2.xiuxian.xiuxian_sect.transaction_service import (
    SectDisbandService,
)
from tests.test_db_backend import db_backend


class SectDisbandServiceTests(unittest.TestCase):
    def setUp(self) -> None:
        self.temp_dir = tempfile.TemporaryDirectory()
        self.database = Path(self.temp_dir.name) / "sect.sqlite3"
        with db_backend.transaction(self.database) as conn:
            conn.execute(
                "CREATE TABLE user_xiuxian ("
                "user_id TEXT PRIMARY KEY, sect_id INTEGER, sect_position INTEGER, "
                "sect_contribution INTEGER)"
            )
            conn.execute(
                "CREATE TABLE sects (sect_id INTEGER PRIMARY KEY, sect_name TEXT, "
                "sect_owner TEXT)"
            )
            conn.execute("INSERT INTO sects VALUES (1, '青云宗', 'owner')")
            conn.execute("INSERT INTO user_xiuxian VALUES ('owner', 1, 0, 100)")
            conn.execute("INSERT INTO user_xiuxian VALUES ('elder', 1, 2, 80)")
            conn.execute("INSERT INTO user_xiuxian VALUES ('outsider', NULL, NULL, 30)")
        self.service = SectDisbandService(self.database)

    def tearDown(self) -> None:
        self.temp_dir.cleanup()

    def row(self, sql, params=()):
        with db_backend.connection(self.database) as conn:
            return conn.execute(sql, params).fetchone()

    def test_disband_unbinds_all_members_and_deletes_sect(self) -> None:
        result = self.service.disband("disband-1", "owner", expected_sect_id=1)

        self.assertEqual(result.status, "disbanded")
        self.assertEqual((result.sect_name, result.member_count), ("青云宗", 2))
        self.assertIsNone(self.row("SELECT sect_id FROM sects WHERE sect_id=1"))
        self.assertEqual(
            self.row("SELECT COUNT(*) FROM user_xiuxian WHERE sect_id=1")[0], 0
        )
        self.assertEqual(
            tuple(
                self.row(
                    "SELECT sect_id, sect_position, sect_contribution "
                    "FROM user_xiuxian WHERE user_id='elder'"
                )
            ),
            (None, None, 0),
        )

    def test_owner_permission_is_rechecked_inside_transaction(self) -> None:
        with db_backend.transaction(self.database) as conn:
            conn.execute("UPDATE sects SET sect_owner='elder' WHERE sect_id=1")

        result = self.service.disband("stale-owner", "owner", expected_sect_id=1)

        self.assertEqual(result.status, "not_owner")
        self.assertEqual(self.row("SELECT COUNT(*) FROM sects WHERE sect_id=1")[0], 1)
        self.assertEqual(self.row("SELECT COUNT(*) FROM user_xiuxian WHERE sect_id=1")[0], 2)

    def test_repeated_operation_is_idempotent_after_sect_deletion(self) -> None:
        first = self.service.disband("repeat", "owner")
        second = self.service.disband("repeat", "owner")

        self.assertEqual(first.status, "disbanded")
        self.assertEqual(second.status, "duplicate")
        self.assertEqual((second.sect_name, second.member_count), ("青云宗", 2))
        self.assertEqual(
            self.row("SELECT COUNT(*) FROM sect_disband_operations")[0], 1
        )

    def test_injected_operation_failure_rolls_back_members_and_sect(self) -> None:
        with db_backend.transaction(self.database) as conn:
            self.service._ensure_operations(conn)
            conn.execute(
                "CREATE TRIGGER fail_disband_operation BEFORE INSERT "
                "ON sect_disband_operations BEGIN "
                "SELECT RAISE(ABORT, 'operation failed'); END"
            )

        with self.assertRaises(db_backend.IntegrityError):
            self.service.disband("fail", "owner")

        self.assertEqual(self.row("SELECT COUNT(*) FROM sects WHERE sect_id=1")[0], 1)
        self.assertEqual(self.row("SELECT COUNT(*) FROM user_xiuxian WHERE sect_id=1")[0], 2)
        self.assertEqual(
            tuple(
                self.row(
                    "SELECT sect_id, sect_position, sect_contribution "
                    "FROM user_xiuxian WHERE user_id='owner'"
                )
            ),
            (1, 0, 100),
        )

    def test_confirm_entry_uses_service_without_legacy_delete(self) -> None:
        source = (
            Path(__file__).parents[1]
            / "nonebot_plugin_xiuxian_2/xiuxian/xiuxian_sect/__init__.py"
        ).read_text(encoding="utf-8")
        start = source.index("async def sect_disband2_confirm")
        end = source.index("@sect_power_top.handle", start)
        handler = source[start:end]

        self.assertIn("sect_disband_service.disband(", handler)
        self.assertNotIn("sql_message.delete_sect(", handler)


if __name__ == "__main__":
    unittest.main()
