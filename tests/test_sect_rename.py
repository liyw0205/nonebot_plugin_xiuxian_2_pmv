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


class SectRenameTests(unittest.TestCase):
    def setUp(self) -> None:
        self.temp_dir = tempfile.TemporaryDirectory()
        self.database = Path(self.temp_dir.name) / "sect.sqlite3"
        with db_backend.transaction(self.database) as conn:
            conn.execute(
                "CREATE TABLE user_xiuxian "
                "(user_id TEXT PRIMARY KEY, sect_id INTEGER, sect_position INTEGER)"
            )
            conn.execute(
                "CREATE TABLE sects "
                "(sect_id INTEGER PRIMARY KEY, sect_name TEXT NOT NULL, "
                "sect_owner TEXT, sect_used_stone INTEGER)"
            )
            conn.execute(
                "CREATE TABLE back "
                "(user_id TEXT, goods_id INTEGER, goods_num INTEGER, bind_num INTEGER)"
            )
            conn.execute("INSERT INTO user_xiuxian VALUES (%s, %s, %s)", ("owner", 1, 0))
            conn.execute("INSERT INTO user_xiuxian VALUES (%s, %s, %s)", ("member", 1, 3))
            conn.execute("INSERT INTO sects VALUES (%s, %s, %s, %s)", (1, "青云宗", "owner", 1000))
            conn.execute("INSERT INTO sects VALUES (%s, %s, %s, %s)", (2, "天音寺", "other", 1000))
            conn.execute("INSERT INTO back VALUES (%s, %s, %s, %s)", ("owner", 1999, 2, 1))
        self.service = SectMembershipService(self.database)

    def tearDown(self) -> None:
        self.temp_dir.cleanup()

    def state(self) -> tuple[str, int, int, int]:
        with db_backend.connection(self.database) as conn:
            sect = conn.execute(
                "SELECT sect_name, sect_used_stone FROM sects WHERE sect_id=%s", (1,)
            ).fetchone()
            card = conn.execute(
                "SELECT goods_num, bind_num FROM back WHERE user_id=%s AND goods_id=%s",
                ("owner", 1999),
            ).fetchone()
        return str(sect[0]), int(sect[1]), int(card[0]), int(card[1])

    def operation_count(self) -> int:
        with db_backend.connection(self.database) as conn:
            exists = conn.execute(
                "SELECT 1 FROM sqlite_master WHERE type='table' AND name=%s",
                ("sect_rename_operations",),
            ).fetchone()
            if exists is None:
                return 0
            return int(
                conn.execute("SELECT COUNT(*) FROM sect_rename_operations").fetchone()[0]
            )

    def test_rename_updates_name_and_consumes_assets_atomically(self) -> None:
        result = self.service.rename_sect("rename-1", "owner", 1, "凌霄宗", 300, 1999)

        self.assertEqual(result.status, "renamed")
        self.assertEqual((result.previous_name, result.new_name), ("青云宗", "凌霄宗"))
        self.assertEqual(self.state(), ("凌霄宗", 700, 1, 0))
        self.assertEqual(self.operation_count(), 1)

    def test_duplicate_operation_does_not_consume_assets_twice(self) -> None:
        first = self.service.rename_sect("rename-repeat", "owner", 1, "凌霄宗", 300, 1999)
        second = self.service.rename_sect("rename-repeat", "owner", 1, "其他名称", 999, 9999)

        self.assertEqual((first.status, second.status), ("renamed", "duplicate"))
        self.assertEqual((second.previous_name, second.new_name), ("青云宗", "凌霄宗"))
        self.assertEqual(self.state(), ("凌霄宗", 700, 1, 0))
        self.assertEqual(self.operation_count(), 1)

    def test_existing_name_leaves_every_asset_unchanged(self) -> None:
        result = self.service.rename_sect("rename-conflict", "owner", 1, "天音寺", 300, 1999)

        self.assertEqual(result.status, "name_exists")
        self.assertEqual(self.state(), ("青云宗", 1000, 2, 1))

    def test_insufficient_stone_leaves_card_and_name_unchanged(self) -> None:
        result = self.service.rename_sect("rename-poor", "owner", 1, "凌霄宗", 1001, 1999)

        self.assertEqual(result.status, "stone_insufficient")
        self.assertEqual(self.state(), ("青云宗", 1000, 2, 1))

    def test_missing_card_leaves_stone_and_name_unchanged(self) -> None:
        with db_backend.transaction(self.database) as conn:
            conn.execute("UPDATE back SET goods_num=0, bind_num=0 WHERE user_id=%s", ("owner",))

        result = self.service.rename_sect("rename-no-card", "owner", 1, "凌霄宗", 300, 1999)

        self.assertEqual(result.status, "card_insufficient")
        self.assertEqual(self.state(), ("青云宗", 1000, 0, 0))

    def test_database_failure_rolls_back_name_stone_and_card(self) -> None:
        with db_backend.transaction(self.database) as conn:
            self.service._ensure_rename_operations(conn)
            conn.execute(
                "CREATE TRIGGER fail_rename BEFORE INSERT ON sect_rename_operations "
                "BEGIN SELECT RAISE(ABORT, 'rename failed'); END"
            )

        with self.assertRaises(db_backend.IntegrityError):
            self.service.rename_sect("rename-fail", "owner", 1, "凌霄宗", 300, 1999)

        self.assertEqual(self.state(), ("青云宗", 1000, 2, 1))
        self.assertEqual(self.operation_count(), 0)

    def test_current_owner_and_membership_are_rechecked(self) -> None:
        with db_backend.transaction(self.database) as conn:
            conn.execute("UPDATE sects SET sect_owner=%s WHERE sect_id=%s", ("member", 1))

        result = self.service.rename_sect("rename-owner", "owner", 1, "凌霄宗", 300, 1999)

        self.assertEqual(result.status, "not_owner")
        self.assertEqual(self.state(), ("青云宗", 1000, 2, 1))

    def test_missing_sect_or_actor_state_changes_are_rejected(self) -> None:
        with db_backend.transaction(self.database) as conn:
            conn.execute("DELETE FROM sects WHERE sect_id=%s", (1,))

        result = self.service.rename_sect("rename-no-sect", "owner", 1, "凌霄宗", 300, 1999)

        self.assertEqual(result.status, "sect_missing")
        self.assertEqual(self.operation_count(), 0)

    def test_name_equal_current_name_is_treated_as_conflict_without_cost(self) -> None:
        result = self.service.rename_sect("rename-same", "owner", 1, "青云宗", 300, 1999)

        self.assertEqual(result.status, "name_exists")
        self.assertEqual(self.state(), ("青云宗", 1000, 2, 1))
        self.assertEqual(self.operation_count(), 0)


if __name__ == "__main__":
    unittest.main()
