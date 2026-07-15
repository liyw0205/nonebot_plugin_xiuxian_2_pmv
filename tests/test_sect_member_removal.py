from __future__ import annotations

import tempfile
import unittest
from pathlib import Path

import nonebot

nonebot.init()

from nonebot_plugin_xiuxian_2.xiuxian.xiuxian_sect.transaction_service import (
    SectMembershipService,
)
from tests.test_db_backend import db_backend


class SectMemberRemovalTests(unittest.TestCase):
    def setUp(self) -> None:
        self.temp_dir = tempfile.TemporaryDirectory()
        self.database = Path(self.temp_dir.name) / "sect.sqlite3"
        with db_backend.transaction(self.database) as conn:
            conn.execute(
                "CREATE TABLE user_xiuxian "
                "(user_id TEXT PRIMARY KEY, sect_id INTEGER, sect_position INTEGER, "
                "user_name TEXT, sect_contribution INTEGER)"
            )
            conn.execute(
                "CREATE TABLE sects "
                "(sect_id INTEGER PRIMARY KEY, sect_name TEXT NOT NULL)"
            )
            conn.executemany(
                "INSERT INTO user_xiuxian VALUES (%s, %s, %s, %s, %s)",
                [
                    ("owner", 1, 0, "宗主", 100),
                    ("elder", 1, 2, "长老", 80),
                    ("member", 1, 5, "弟子", 60),
                    ("peer", 1, 2, "同阶", 40),
                    ("outsider", 2, 5, "外宗弟子", 20),
                    ("wanderer", None, None, "散修", 0),
                ],
            )
            conn.executemany(
                "INSERT INTO sects VALUES (%s, %s)",
                [(1, "青云宗"), (2, "天音寺")],
            )
        self.service = SectMembershipService(self.database)

    def tearDown(self) -> None:
        self.temp_dir.cleanup()

    def state(self, user_id: str):
        with db_backend.connection(self.database) as conn:
            row = conn.execute(
                "SELECT sect_id, sect_position, sect_contribution "
                "FROM user_xiuxian WHERE user_id=%s",
                (user_id,),
            ).fetchone()
        return tuple(row) if row else None

    def test_member_can_leave_and_contribution_is_cleared(self) -> None:
        result = self.service.leave_sect("leave-1", "member")

        self.assertEqual(result.status, "left")
        self.assertEqual(result.sect_name, "青云宗")
        self.assertEqual(self.state("member"), (None, None, 0))

    def test_owner_cannot_leave(self) -> None:
        result = self.service.leave_sect("leave-owner", "owner")

        self.assertEqual(result.status, "owner_cannot_leave")
        self.assertEqual(self.state("owner"), (1, 0, 100))

    def test_elder_can_kick_lower_member(self) -> None:
        result = self.service.kick_member(
            "kick-1", "elder", "member", manager_max_position=2
        )

        self.assertEqual(result.status, "kicked")
        self.assertEqual(result.sect_name, "青云宗")
        self.assertEqual(self.state("member"), (None, None, 0))

    def test_kick_rechecks_rank_and_membership(self) -> None:
        peer = self.service.kick_member(
            "kick-peer", "elder", "peer", manager_max_position=2
        )
        outsider = self.service.kick_member(
            "kick-outsider", "elder", "outsider", manager_max_position=2
        )

        self.assertEqual(peer.status, "target_not_lower")
        self.assertEqual(outsider.status, "different_sect")
        self.assertEqual(self.state("peer"), (1, 2, 40))
        self.assertEqual(self.state("outsider"), (2, 5, 20))

    def test_duplicate_operations_do_not_repeat_mutation(self) -> None:
        first = self.service.kick_member(
            "kick-repeat", "elder", "member", manager_max_position=2
        )
        second = self.service.kick_member(
            "kick-repeat", "elder", "member", manager_max_position=2
        )

        self.assertEqual((first.status, second.status), ("kicked", "duplicate"))
        self.assertEqual(self.state("member"), (None, None, 0))

    def test_database_failure_rolls_back_member_state(self) -> None:
        with db_backend.transaction(self.database) as conn:
            self.service._ensure_member_removal_operations(conn)
            conn.execute(
                "CREATE TRIGGER fail_removal BEFORE INSERT ON "
                "sect_member_removal_operations "
                "BEGIN SELECT RAISE(ABORT, 'removal failed'); END"
            )

        with self.assertRaises(db_backend.IntegrityError):
            self.service.kick_member(
                "kick-fail", "elder", "member", manager_max_position=2
            )

        self.assertEqual(self.state("member"), (1, 5, 60))


if __name__ == "__main__":
    unittest.main()
