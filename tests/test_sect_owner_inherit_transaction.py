from __future__ import annotations

import tempfile
import unittest
from pathlib import Path

import nonebot

nonebot.init()

from nonebot_plugin_xiuxian_2.xiuxian.xiuxian_sect.transaction_service import (
    SectOwnerInheritService,
)
from tests.test_db_backend import db_backend


class SectOwnerInheritServiceTests(unittest.TestCase):
    def setUp(self) -> None:
        self.temp_dir = tempfile.TemporaryDirectory()
        self.database = Path(self.temp_dir.name) / "sect.sqlite3"
        with db_backend.transaction(self.database) as conn:
            conn.execute(
                "CREATE TABLE user_xiuxian ("
                "user_id TEXT PRIMARY KEY, sect_id INTEGER, sect_position INTEGER, "
                "user_name TEXT, sect_contribution INTEGER)"
            )
            conn.execute(
                "CREATE TABLE sects ("
                "sect_id INTEGER PRIMARY KEY, sect_name TEXT, sect_owner TEXT, "
                "join_open INTEGER, closed INTEGER)"
            )
            conn.execute(
                "INSERT INTO sects VALUES (%s, %s, %s, %s, %s)",
                (1, "青云宗", None, 0, 1),
            )
            conn.execute(
                "INSERT INTO user_xiuxian VALUES (%s, %s, %s, %s, %s)",
                ("vice", 1, 1, "副宗主", 50),
            )
            conn.execute(
                "INSERT INTO user_xiuxian VALUES (%s, %s, %s, %s, %s)",
                ("elder", 1, 2, "长老", 100),
            )
        self.service = SectOwnerInheritService(self.database)

    def tearDown(self) -> None:
        self.temp_dir.cleanup()

    def row(self, sql, params=()):
        with db_backend.connection(self.database) as conn:
            return conn.execute(sql, params).fetchone()

    def test_highest_priority_candidate_inherits_atomically(self) -> None:
        result = self.service.inherit("inherit-1", "vice", expected_sect_id=1)

        self.assertEqual(result.status, "inherited")
        self.assertEqual(result.actor_name, "副宗主")
        self.assertEqual(
            tuple(self.row("SELECT sect_owner, join_open, closed FROM sects WHERE sect_id=1")),
            ("vice", 1, 0),
        )
        self.assertEqual(
            self.row("SELECT sect_position FROM user_xiuxian WHERE user_id='vice'")[0],
            0,
        )

    def test_lower_priority_candidate_is_rejected(self) -> None:
        result = self.service.inherit("inherit-low", "elder")

        self.assertEqual(result.status, "higher_priority")
        self.assertEqual(
            tuple(self.row("SELECT sect_owner, join_open, closed FROM sects WHERE sect_id=1")),
            (None, 0, 1),
        )

    def test_auto_path_can_limit_candidates_to_active_members(self) -> None:
        result = self.service.inherit(
            "inherit-active", "elder", eligible_user_ids=("elder",)
        )

        self.assertEqual(result.status, "inherited")
        self.assertEqual(self.row("SELECT sect_owner FROM sects WHERE sect_id=1")[0], "elder")

    def test_repeated_operation_is_idempotent(self) -> None:
        first = self.service.inherit("inherit-repeat", "vice")
        second = self.service.inherit("inherit-repeat", "vice")

        self.assertEqual(first.status, "inherited")
        self.assertEqual(second.status, "duplicate")
        self.assertEqual(
            self.row("SELECT COUNT(*) FROM sect_owner_inherit_operations")[0], 1
        )

    def test_database_failure_rolls_back_member_and_sect(self) -> None:
        with db_backend.transaction(self.database) as conn:
            conn.execute(
                "CREATE TRIGGER fail_inherit BEFORE UPDATE ON sects "
                "BEGIN SELECT RAISE(ABORT, 'inherit failed'); END"
            )

        with self.assertRaises(db_backend.IntegrityError):
            self.service.inherit("inherit-fail", "vice")

        self.assertEqual(
            tuple(self.row("SELECT sect_owner, join_open, closed FROM sects WHERE sect_id=1")),
            (None, 0, 1),
        )
        self.assertEqual(
            self.row("SELECT sect_position FROM user_xiuxian WHERE user_id='vice'")[0],
            1,
        )

    def test_manual_and_automatic_paths_use_service(self) -> None:
        source = (
            Path(__file__).parents[1]
            / "nonebot_plugin_xiuxian_2/xiuxian/xiuxian_sect/__init__.py"
        ).read_text(encoding="utf-8")
        auto_start = source.index("async def auto_handle_inactive_sect_owners")
        auto_end = source.index("@sect_help.handle", auto_start)
        manual_start = source.index("async def sect_inherit_")
        manual_end = source.index("@sect_disband.handle", manual_start)
        auto_handler = source[auto_start:auto_end]
        manual_handler = source[manual_start:manual_end]

        self.assertIn("sect_owner_inherit_service.inherit(", auto_handler)
        self.assertIn("sect_owner_inherit_service.inherit(", manual_handler)
        for old_call in (
            "sql_message.update_sect_closed_status(",
            "sql_message.update_usr_sect(",
            "sql_message.update_sect_owner(",
        ):
            self.assertNotIn(old_call, auto_handler)
            self.assertNotIn(old_call, manual_handler)


if __name__ == "__main__":
    unittest.main()
