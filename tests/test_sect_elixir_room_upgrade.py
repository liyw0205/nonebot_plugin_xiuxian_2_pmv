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


class SectElixirRoomUpgradeTests(unittest.TestCase):
    def setUp(self) -> None:
        self.temp_dir = tempfile.TemporaryDirectory()
        self.database = Path(self.temp_dir.name) / "sect-elixir-room.sqlite3"
        with db_backend.transaction(self.database) as conn:
            conn.execute(
                "CREATE TABLE user_xiuxian "
                "(user_id TEXT PRIMARY KEY, sect_id INTEGER, sect_position INTEGER)"
            )
            conn.execute(
                """
                CREATE TABLE sects (
                    sect_id INTEGER PRIMARY KEY,
                    sect_owner TEXT,
                    elixir_room_level INTEGER,
                    sect_used_stone INTEGER,
                    sect_scale INTEGER
                )
                """
            )
            conn.execute(
                "INSERT INTO user_xiuxian VALUES (%s, %s, %s)",
                ("owner", 1, 0),
            )
            conn.execute(
                "INSERT INTO user_xiuxian VALUES (%s, %s, %s)",
                ("member", 1, 3),
            )
            conn.execute(
                "INSERT INTO sects VALUES (%s, %s, %s, %s, %s)",
                (1, "owner", 1, 1000, 2000),
            )
        self.service = SectMembershipService(self.database)

    def tearDown(self) -> None:
        self.temp_dir.cleanup()

    def row(self):
        with db_backend.connection(self.database) as conn:
            return tuple(
                conn.execute(
                    "SELECT elixir_room_level, sect_used_stone, sect_scale "
                    "FROM sects WHERE sect_id=1"
                ).fetchone()
            )

    def test_upgrade_deducts_assets_and_changes_level_atomically(self) -> None:
        result = self.service.upgrade_elixir_room(
            "upgrade-1", "owner", 1, 1, 2, 100, 200
        )
        self.assertEqual(result.status, "upgraded")
        self.assertEqual(self.row(), (2, 900, 1800))

    def test_duplicate_operation_does_not_deduct_twice(self) -> None:
        self.service.upgrade_elixir_room(
            "upgrade-repeat", "owner", 1, 1, 2, 100, 200
        )
        result = self.service.upgrade_elixir_room(
            "upgrade-repeat", "owner", 1, 1, 2, 100, 200
        )
        self.assertEqual(result.status, "duplicate")
        self.assertEqual(self.row(), (2, 900, 1800))

    def test_owner_is_checked_against_member_and_sect_records(self) -> None:
        result = self.service.upgrade_elixir_room(
            "member", "member", 1, 1, 2, 100, 200
        )
        self.assertEqual(result.status, "not_owner")
        with db_backend.transaction(self.database) as conn:
            conn.execute(
                "UPDATE user_xiuxian SET sect_position=0 WHERE user_id=%s",
                ("member",),
            )
        result = self.service.upgrade_elixir_room(
            "fake-owner", "member", 1, 1, 2, 100, 200
        )
        self.assertEqual(result.status, "not_owner")
        self.assertEqual(self.row(), (1, 1000, 2000))

    def test_level_and_balances_are_checked_inside_transaction(self) -> None:
        self.assertEqual(
            self.service.upgrade_elixir_room(
                "level", "owner", 1, 0, 1, 100, 200
            ).status,
            "level_changed",
        )
        self.assertEqual(
            self.service.upgrade_elixir_room(
                "stone", "owner", 1, 1, 2, 1001, 200
            ).status,
            "stone_insufficient",
        )
        self.assertEqual(
            self.service.upgrade_elixir_room(
                "scale", "owner", 1, 1, 2, 100, 2001
            ).status,
            "scale_insufficient",
        )
        self.assertEqual(self.row(), (1, 1000, 2000))

    def test_database_failure_rolls_back_all_fields(self) -> None:
        with db_backend.transaction(self.database) as conn:
            self.service._ensure_elixir_room_operations(conn)
            conn.execute(
                """
                CREATE TRIGGER fail_operation
                BEFORE INSERT ON sect_elixir_room_operations
                BEGIN SELECT RAISE(ABORT, 'operation failed'); END
                """
            )
        with self.assertRaises(db_backend.IntegrityError):
            self.service.upgrade_elixir_room(
                "upgrade-fail", "owner", 1, 1, 2, 100, 200
            )
        self.assertEqual(self.row(), (1, 1000, 2000))


if __name__ == "__main__":
    unittest.main()
