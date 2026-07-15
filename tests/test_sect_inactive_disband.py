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


class SectInactiveDisbandTests(unittest.TestCase):
    checked_at = "2026-07-14 12:05:20"

    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.database = Path(self.temp.name) / "sect.db"
        with db_backend.transaction(self.database) as conn:
            conn.execute(
                "CREATE TABLE sects("
                "sect_id INTEGER PRIMARY KEY,sect_name TEXT,sect_owner TEXT,closed INTEGER)"
            )
            conn.execute(
                "CREATE TABLE user_xiuxian("
                "user_id TEXT PRIMARY KEY,user_name TEXT,sect_id INTEGER,"
                "sect_position INTEGER,sect_contribution INTEGER)"
            )
            conn.execute(
                "CREATE TABLE user_cd(user_id TEXT PRIMARY KEY,last_check_info_time TEXT)"
            )
        self.service = SectDisbandService(self.database)

    def tearDown(self):
        self.temp.cleanup()

    def add_sect(self, sect_id, name, owner, closed, members=()):
        with db_backend.transaction(self.database) as conn:
            conn.execute(
                "INSERT INTO sects VALUES(%s,%s,%s,%s)",
                (sect_id, name, owner, int(closed)),
            )
            for user_id, position, last_active in members:
                conn.execute(
                    "INSERT INTO user_xiuxian VALUES(%s,%s,%s,%s,100)",
                    (user_id, user_id, sect_id, position),
                )
                if last_active is not None:
                    conn.execute(
                        "INSERT INTO user_cd VALUES(%s,%s)",
                        (user_id, last_active),
                    )

    def disband(
        self,
        operation,
        sect_id,
        reason,
        name,
        owner,
        closed,
        members,
        active=(),
        **kwargs,
    ):
        return self.service.disband_inactive(
            operation,
            sect_id,
            reason,
            expected_sect_name=name,
            expected_owner_id=owner,
            expected_closed=closed,
            expected_member_ids=members,
            expected_active_candidate_ids=active,
            checked_at=kwargs.get("checked_at", self.checked_at),
            inactivity_days=30,
        )

    def scalar(self, sql, params=()):
        with db_backend.connection(self.database) as conn:
            row = conn.execute(sql, params).fetchone()
            return None if row is None else row[0]

    def test_empty_closed_sect_disbands_and_replays(self):
        self.add_sect(1, "空宗", None, True)
        first = self.disband("empty", 1, "empty", "空宗", None, True, ())
        duplicate = self.disband("empty", 1, "empty", "空宗", None, True, ())
        conflict = self.disband("empty", 1, "empty", "改名宗", None, True, ())

        self.assertEqual(
            (first.status, duplicate.status, conflict.status),
            ("disbanded", "duplicate", "operation_conflict"),
        )
        self.assertEqual((first.member_count, duplicate.reason), (0, "empty"))
        self.assertIsNone(self.scalar("SELECT sect_id FROM sects WHERE sect_id=1"))
        self.assertEqual(
            self.scalar("SELECT COUNT(*) FROM sect_inactive_disband_operations"), 1
        )

    def test_closed_sect_without_active_successor_clears_all_members(self):
        members = (
            ("former", 2, "2026-05-01 00:00:00"),
            ("elder", 1, "2026-06-01 00:00:00"),
        )
        self.add_sect(2, "旧宗", None, True, members)
        result = self.disband(
            "no-successor",
            2,
            "no_active_successor",
            "旧宗",
            None,
            True,
            ("former", "elder"),
        )

        self.assertEqual((result.status, result.member_count), ("disbanded", 2))
        self.assertIsNone(self.scalar("SELECT sect_id FROM sects WHERE sect_id=2"))
        self.assertEqual(
            self.scalar("SELECT COUNT(*) FROM user_xiuxian WHERE sect_id=2"), 0
        )
        self.assertEqual(
            self.scalar(
                "SELECT sect_contribution FROM user_xiuxian WHERE user_id='elder'"
            ),
            0,
        )

    def test_active_candidate_or_changed_members_block_stale_deletion(self):
        self.add_sect(
            3,
            "活跃宗",
            None,
            True,
            (("candidate", 1, "2026-07-14 10:00:00"),),
        )
        active_changed = self.disband(
            "active-changed",
            3,
            "no_active_successor",
            "活跃宗",
            None,
            True,
            ("candidate",),
            (),
        )
        self.assertEqual(active_changed.status, "candidates_changed")
        self.assertEqual(self.scalar("SELECT COUNT(*) FROM sects WHERE sect_id=3"), 1)

        self.add_sect(
            4,
            "成员变化宗",
            None,
            True,
            (("member", 2, "2026-05-01 00:00:00"),),
        )
        members_changed = self.disband(
            "members-changed", 4, "empty", "成员变化宗", None, True, ()
        )
        self.assertEqual(members_changed.status, "members_changed")
        self.assertEqual(self.scalar("SELECT COUNT(*) FROM sects WHERE sect_id=4"), 1)

    def test_only_inactive_owner_can_trigger_sole_owner_disband(self):
        self.add_sect(
            5,
            "独行宗",
            "owner-old",
            False,
            (("owner-old", 0, "2026-06-01 00:00:00"),),
        )
        inactive = self.disband(
            "sole-old",
            5,
            "inactive_sole_owner",
            "独行宗",
            "owner-old",
            False,
            ("owner-old",),
        )
        self.assertEqual(inactive.status, "disbanded")

        self.add_sect(
            6,
            "新宗",
            "owner-new",
            False,
            (("owner-new", 0, "2026-07-14 10:00:00"),),
        )
        active = self.disband(
            "sole-new",
            6,
            "inactive_sole_owner",
            "新宗",
            "owner-new",
            False,
            ("owner-new",),
        )
        self.assertEqual(active.status, "condition_changed")
        self.assertEqual(self.scalar("SELECT COUNT(*) FROM sects WHERE sect_id=6"), 1)

    def test_operation_failure_rolls_back_sect_and_members(self):
        self.add_sect(
            7,
            "回滚宗",
            "owner",
            False,
            (("owner", 0, "2026-05-01 00:00:00"),),
        )
        with db_backend.transaction(self.database) as conn:
            self.service._ensure_inactive_operations(conn)
            conn.execute(
                "CREATE TRIGGER fail_inactive_disband BEFORE INSERT "
                "ON sect_inactive_disband_operations BEGIN "
                "SELECT RAISE(ABORT,'failed'); END"
            )
        with self.assertRaises(db_backend.IntegrityError):
            self.disband(
                "rollback",
                7,
                "inactive_sole_owner",
                "回滚宗",
                "owner",
                False,
                ("owner",),
            )
        self.assertEqual(self.scalar("SELECT COUNT(*) FROM sects WHERE sect_id=7"), 1)
        self.assertEqual(
            self.scalar("SELECT sect_id FROM user_xiuxian WHERE user_id='owner'"), 7
        )
        self.assertEqual(
            self.scalar(
                "SELECT sect_contribution FROM user_xiuxian WHERE user_id='owner'"
            ),
            100,
        )

    def test_automatic_entry_uses_stable_transactional_services(self):
        source = (
            Path(__file__).parents[1]
            / "nonebot_plugin_xiuxian_2/xiuxian/xiuxian_sect/__init__.py"
        ).read_text(encoding="utf-8")
        handler = source.split("async def auto_handle_inactive_sect_owners", 1)[1].split(
            "@sect_help.handle", 1
        )[0]
        self.assertEqual(handler.count("sect_disband_service.disband_inactive("), 3)
        self.assertNotIn("sql_message.delete_sect(", handler)
        self.assertNotIn("time.time_ns()", handler)
        self.assertIn("maintenance_key", handler)


if __name__ == "__main__":
    unittest.main()
