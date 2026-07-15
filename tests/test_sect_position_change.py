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


def _create_schema(conn) -> None:
    conn.execute(
        """
        CREATE TABLE user_xiuxian (
            user_id TEXT PRIMARY KEY,
            user_name TEXT,
            sect_id INTEGER,
            sect_position INTEGER,
            sect_contribution INTEGER DEFAULT 0
        )
        """
    )


class SectPositionChangeTests(unittest.TestCase):
    def setUp(self) -> None:
        self.tempdir = tempfile.TemporaryDirectory()
        self.db_path = Path(self.tempdir.name) / "sect_position_change.db"
        with db_backend.transaction(self.db_path) as conn:
            _create_schema(conn)
            conn.executemany(
                "INSERT INTO user_xiuxian (user_id, user_name, sect_id, sect_position, sect_contribution) VALUES (%s, %s, %s, %s, %s)",
                [
                    ("owner", "宗主", 1, 0, 0),
                    ("vice", "副宗主", 1, 1, 0),
                    ("elder", "长老", 1, 2, 0),
                    ("member", "弟子", 1, 5, 0),
                    ("peer", "同阶", 1, 2, 0),
                    ("hall_a", "堂主甲", 1, 3, 0),
                    ("hall_b", "堂主乙", 1, 3, 0),
                    ("outsider", "外宗", 2, 5, 0),
                    ("wanderer", "散修", None, None, 0),
                ],
            )
        self.service = SectMembershipService(self.db_path)
        self.position_limits = {0: 1, 1: 1, 2: 3, 3: 2, 4: 0, 5: 0, 6: 1}

    def tearDown(self) -> None:
        self.tempdir.cleanup()

    def _position(self, user_id: str):
        with db_backend.connection(self.db_path) as conn:
            row = conn.execute(
                "SELECT sect_position FROM user_xiuxian WHERE user_id=%s",
                (user_id,),
            ).fetchone()
        return row[0] if row else None

    def test_owner_can_change_member_position(self) -> None:
        result = self.service.change_position(
            "pos-owner",
            "owner",
            "member",
            4,
            self.position_limits,
            manager_max_position=2,
        )

        self.assertEqual(result.status, "changed")
        self.assertEqual(self._position("member"), 4)

    def test_vice_can_change_member_position(self) -> None:
        result = self.service.change_position(
            "pos-vice",
            "vice",
            "member",
            4,
            self.position_limits,
            manager_max_position=2,
        )
        self.assertEqual(result.status, "changed")

    def test_elder_can_change_member_position(self) -> None:
        result = self.service.change_position(
            "pos-elder",
            "elder",
            "member",
            4,
            self.position_limits,
            manager_max_position=2,
        )
        self.assertEqual(result.status, "changed")

    def test_non_manager_rejected(self) -> None:
        result = self.service.change_position(
            "pos-non-manager",
            "member",
            "outsider",
            5,
            self.position_limits,
            manager_max_position=2,
        )
        self.assertEqual(result.status, "actor_not_manager")

    def test_target_not_in_same_sect(self) -> None:
        result = self.service.change_position(
            "pos-other",
            "elder",
            "outsider",
            4,
            self.position_limits,
            manager_max_position=2,
        )
        self.assertEqual(result.status, "target_not_member")

    def test_self_target_rejected(self) -> None:
        result = self.service.change_position(
            "pos-self",
            "elder",
            "elder",
            4,
            self.position_limits,
            manager_max_position=2,
        )
        self.assertEqual(result.status, "self_target")

    def test_target_must_be_below_actor(self) -> None:
        result = self.service.change_position(
            "pos-peer",
            "elder",
            "peer",
            4,
            self.position_limits,
            manager_max_position=2,
        )
        self.assertEqual(result.status, "target_not_below_actor")

    def test_new_position_must_be_below_actor(self) -> None:
        result = self.service.change_position(
            "pos-high",
            "elder",
            "member",
            2,
            self.position_limits,
            manager_max_position=2,
        )
        self.assertEqual(result.status, "position_not_below_actor")

    def test_position_limit_enforced(self) -> None:
        result = self.service.change_position(
            "pos-full",
            "owner",
            "member",
            3,
            self.position_limits,
            manager_max_position=2,
        )
        self.assertEqual(result.status, "position_full")
        self.assertEqual(self._position("member"), 5)

    def test_unchanged_position_returns_unchanged(self) -> None:
        result = self.service.change_position(
            "pos-same",
            "owner",
            "member",
            5,
            self.position_limits,
            manager_max_position=2,
        )
        self.assertEqual(result.status, "unchanged")

    def test_duplicate_is_idempotent(self) -> None:
        first = self.service.change_position(
            "pos-dup",
            "owner",
            "member",
            4,
            self.position_limits,
            manager_max_position=2,
        )
        second = self.service.change_position(
            "pos-dup",
            "owner",
            "member",
            4,
            self.position_limits,
            manager_max_position=2,
        )
        self.assertEqual(first.status, "changed")
        self.assertEqual(second.status, "duplicate")
        self.assertEqual(self._position("member"), 4)

    def test_invalid_position_rejected(self) -> None:
        result = self.service.change_position(
            "pos-invalid",
            "owner",
            "member",
            99,
            self.position_limits,
            manager_max_position=2,
        )
        self.assertEqual(result.status, "invalid_position")

    def test_change_rolls_back_on_update_failure(self) -> None:
        with db_backend.transaction(self.db_path) as conn:
            conn.execute(
                "CREATE TRIGGER position_fail AFTER UPDATE ON user_xiuxian WHEN NEW.user_id='member' BEGIN SELECT RAISE(FAIL, 'boom'); END"
            )

        with self.assertRaises(db_backend.Error):
            self.service.change_position(
                "pos-fail",
                "owner",
                "member",
                4,
                self.position_limits,
                manager_max_position=2,
            )

        self.assertEqual(self._position("member"), 5)


if __name__ == "__main__":
    unittest.main()
