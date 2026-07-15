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


class SectFairylandUpgradeTests(unittest.TestCase):
    def setUp(self) -> None:
        self.temp_dir = tempfile.TemporaryDirectory()
        self.database = Path(self.temp_dir.name) / "sect-upgrade.sqlite3"
        with db_backend.transaction(self.database) as conn:
            conn.execute(
                "CREATE TABLE user_xiuxian (user_id TEXT PRIMARY KEY, sect_id INTEGER, sect_position INTEGER)"
            )
            conn.execute(
                """
                CREATE TABLE sects (
                    sect_id INTEGER PRIMARY KEY,
                    sect_owner TEXT,
                    sect_fairyland INTEGER,
                    sect_used_stone INTEGER,
                    sect_materials INTEGER
                )
                """
            )
            conn.execute("INSERT INTO user_xiuxian VALUES (%s, %s, %s)", ("owner", 1, 0))
            conn.execute("INSERT INTO user_xiuxian VALUES (%s, %s, %s)", ("member", 1, 3))
            conn.execute("INSERT INTO sects VALUES (%s, %s, %s, %s, %s)", (1, "owner", 1, 1000, 2000))
        self.service = SectMembershipService(self.database)

    def tearDown(self) -> None:
        self.temp_dir.cleanup()

    def row(self):
        with db_backend.connection(self.database) as conn:
            return tuple(conn.execute("SELECT sect_fairyland, sect_used_stone, sect_materials FROM sects WHERE sect_id=1").fetchone())

    def test_upgrade_deducts_both_assets_and_changes_level_atomically(self) -> None:
        result = self.service.upgrade_fairyland("upgrade-1", "owner", 1, 1, 2, 100, 200)
        self.assertEqual(result.status, "upgraded")
        self.assertEqual(self.row(), (2, 900, 1800))

    def test_duplicate_operation_does_not_deduct_twice(self) -> None:
        self.service.upgrade_fairyland("upgrade-repeat", "owner", 1, 1, 2, 100, 200)
        result = self.service.upgrade_fairyland("upgrade-repeat", "owner", 1, 1, 2, 100, 200)
        self.assertEqual(result.status, "duplicate")
        self.assertEqual(self.row(), (2, 900, 1800))

    def test_permissions_level_and_balances_are_checked_inside_transaction(self) -> None:
        self.assertEqual(self.service.upgrade_fairyland("member", "member", 1, 1, 2, 100, 200).status, "not_owner")
        self.assertEqual(self.service.upgrade_fairyland("level", "owner", 1, 0, 1, 100, 200).status, "level_changed")
        self.assertEqual(self.service.upgrade_fairyland("stone", "owner", 1, 1, 2, 1001, 200).status, "stone_insufficient")
        self.assertEqual(self.service.upgrade_fairyland("materials", "owner", 1, 1, 2, 100, 2001).status, "materials_insufficient")
        self.assertEqual(self.row(), (1, 1000, 2000))

    def test_database_failure_rolls_back_all_fields(self) -> None:
        with db_backend.transaction(self.database) as conn:
            self.service._ensure_fairyland_operations(conn)
            conn.execute(
                """
                CREATE TRIGGER fail_operation BEFORE INSERT ON sect_fairyland_operations
                BEGIN SELECT RAISE(ABORT, 'operation failed'); END
                """
            )
        with self.assertRaises(db_backend.IntegrityError):
            self.service.upgrade_fairyland("upgrade-fail", "owner", 1, 1, 2, 100, 200)
        self.assertEqual(self.row(), (1, 1000, 2000))


if __name__ == "__main__":
    unittest.main()
