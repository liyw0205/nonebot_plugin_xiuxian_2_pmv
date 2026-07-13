from __future__ import annotations

import tempfile
import unittest
from pathlib import Path

import nonebot

nonebot.init()

from nonebot_plugin_xiuxian_2.xiuxian.xiuxian_sect.close_mountain_service import (
    SectCloseMountainService,
)
from tests.test_db_backend import db_backend


class SectCloseMountainServiceTests(unittest.TestCase):
    def setUp(self) -> None:
        self.temp_dir = tempfile.TemporaryDirectory()
        self.database = Path(self.temp_dir.name) / "sect.sqlite3"
        with db_backend.transaction(self.database) as conn:
            conn.execute(
                "CREATE TABLE user_xiuxian ("
                "user_id TEXT PRIMARY KEY, sect_id INTEGER, "
                "sect_position INTEGER, user_name TEXT, sect_contribution INTEGER)"
            )
            conn.execute(
                "CREATE TABLE sects ("
                "sect_id INTEGER PRIMARY KEY, sect_name TEXT, sect_owner TEXT, "
                "join_open INTEGER, closed INTEGER)"
            )
            conn.execute(
                "INSERT INTO sects VALUES (%s, %s, %s, %s, %s)",
                (1, "青云宗", "owner", 1, 0),
            )
            conn.execute(
                "INSERT INTO user_xiuxian VALUES (%s, %s, %s, %s, %s)",
                ("owner", 1, 0, "旧宗主", 100),
            )
            conn.execute(
                "INSERT INTO user_xiuxian VALUES (%s, %s, %s, %s, %s)",
                ("elder", 1, 2, "长老", 80),
            )
        self.service = SectCloseMountainService(self.database)

    def tearDown(self) -> None:
        self.temp_dir.cleanup()

    def row(self, sql, params=()):
        with db_backend.connection(self.database) as conn:
            return conn.execute(sql, params).fetchone()

    def test_close_updates_all_state_in_one_transaction(self) -> None:
        result = self.service.close("close-1", "owner", expected_sect_id=1)

        self.assertEqual(result.status, "closed")
        sect = self.row(
            "SELECT sect_owner, join_open, closed FROM sects WHERE sect_id=%s", (1,)
        )
        self.assertEqual(tuple(sect), (None, 0, 1))
        self.assertEqual(
            self.row(
                "SELECT sect_position FROM user_xiuxian WHERE user_id=%s", ("owner",)
            )[0],
            2,
        )

    def test_repeated_operation_is_idempotent(self) -> None:
        first = self.service.close("close-repeat", "owner")
        second = self.service.close("close-repeat", "owner")

        self.assertEqual(first.status, "closed")
        self.assertEqual(second.status, "duplicate")
        self.assertEqual(
            self.row("SELECT COUNT(*) FROM sect_close_mountain_operations")[0], 1
        )

    def test_stale_actor_is_rejected(self) -> None:
        with db_backend.transaction(self.database) as conn:
            conn.execute("UPDATE sects SET sect_owner=%s WHERE sect_id=%s", ("elder", 1))

        result = self.service.close("stale", "owner")

        self.assertEqual(result.status, "not_owner")
        self.assertEqual(
            tuple(self.row("SELECT sect_owner, join_open, closed FROM sects WHERE sect_id=1")),
            ("elder", 1, 0),
        )

    def test_database_failure_rolls_back_member_and_sect(self) -> None:
        with db_backend.transaction(self.database) as conn:
            conn.execute(
                "CREATE TRIGGER fail_close BEFORE UPDATE ON sects "
                "BEGIN SELECT RAISE(ABORT, 'close failed'); END"
            )

        with self.assertRaises(db_backend.IntegrityError):
            self.service.close("close-fail", "owner")

        self.assertEqual(
            tuple(self.row("SELECT sect_owner, join_open, closed FROM sects WHERE sect_id=1")),
            ("owner", 1, 0),
        )
        self.assertEqual(
            self.row("SELECT sect_position FROM user_xiuxian WHERE user_id='owner'")[0],
            0,
        )

    def test_manual_and_automatic_paths_use_service(self) -> None:
        source = (
            Path(__file__).parents[1]
            / "nonebot_plugin_xiuxian_2/xiuxian/xiuxian_sect/__init__.py"
        ).read_text(encoding="utf-8")
        auto_start = source.index("async def auto_handle_inactive_sect_owners")
        auto_end = source.index("@sect_help.handle", auto_start)
        manual_start = source.index("async def sect_close_mountain2_confirm")
        manual_end = source.index("@sect_inherit.handle", manual_start)
        auto_handler = source[auto_start:auto_end]
        manual_handler = source[manual_start:manual_end]

        self.assertIn("sect_close_mountain_service.close(", auto_handler)
        self.assertIn("sect_close_mountain_service.close(", manual_handler)
        for old_call in (
            "sql_message.update_sect_closed_status(",
            "sql_message.update_usr_sect(",
            "sql_message.update_sect_owner(",
        ):
            self.assertNotIn(old_call, auto_handler)
            self.assertNotIn(old_call, manual_handler)


if __name__ == "__main__":
    unittest.main()
