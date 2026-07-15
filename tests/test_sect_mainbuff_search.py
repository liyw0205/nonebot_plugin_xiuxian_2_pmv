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


class SectMainbuffSearchTests(unittest.TestCase):
    def setUp(self) -> None:
        self.temp_dir = tempfile.TemporaryDirectory()
        self.database = Path(self.temp_dir.name) / "sect-mainbuff.sqlite3"
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
                    sect_used_stone INTEGER,
                    sect_materials INTEGER,
                    mainbuff TEXT,
                    secbuff TEXT
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
                "INSERT INTO sects VALUES (%s, %s, %s, %s, %s, %s)",
                (1, "owner", 1000, 2000, "[101]", "[201]"),
            )
        self.service = SectMembershipService(self.database)

    def tearDown(self) -> None:
        self.temp_dir.cleanup()

    def row(self):
        with db_backend.connection(self.database) as conn:
            return tuple(
                conn.execute(
                    "SELECT sect_used_stone, sect_materials, mainbuff, secbuff "
                    "FROM sects WHERE sect_id=1"
                ).fetchone()
            )

    def apply(self, operation_id="search-1", **overrides):
        values = {
            "actor_id": "owner",
            "sect_id": 1,
            "buff_type": "main",
            "expected_value": "[101]",
            "new_value": "[101,102]",
            "stone_cost": 100,
            "materials_cost": 200,
        }
        values.update(overrides)
        return self.service.apply_buff_search(operation_id, **values)

    def test_search_deducts_assets_and_updates_list_atomically(self) -> None:
        result = self.apply()
        self.assertEqual(result.status, "applied")
        self.assertEqual(self.row(), (900, 1800, "[101,102]", "[201]"))

    def test_duplicate_operation_does_not_deduct_twice(self) -> None:
        self.apply("search-repeat")
        result = self.apply("search-repeat")
        self.assertEqual(result.status, "duplicate")
        self.assertEqual(self.row(), (900, 1800, "[101,102]", "[201]"))

    def test_owner_and_list_version_are_checked_inside_transaction(self) -> None:
        self.assertEqual(
            self.apply("member", actor_id="member").status,
            "not_owner",
        )
        self.assertEqual(
            self.apply("changed", expected_value="[999]").status,
            "buff_changed",
        )
        self.assertEqual(self.row(), (1000, 2000, "[101]", "[201]"))

    def test_balances_are_checked_inside_transaction(self) -> None:
        self.assertEqual(
            self.apply("stone", stone_cost=1001).status,
            "stone_insufficient",
        )
        self.assertEqual(
            self.apply("materials", materials_cost=2001).status,
            "materials_insufficient",
        )
        self.assertEqual(self.row(), (1000, 2000, "[101]", "[201]"))

    def test_database_failure_rolls_back_assets_and_list(self) -> None:
        with db_backend.transaction(self.database) as conn:
            self.service._ensure_buff_search_operations(conn)
            conn.execute(
                """
                CREATE TRIGGER fail_operation
                BEFORE INSERT ON sect_buff_search_operations
                BEGIN SELECT RAISE(ABORT, 'operation failed'); END
                """
            )
        with self.assertRaises(db_backend.IntegrityError):
            self.apply("search-fail")
        self.assertEqual(self.row(), (1000, 2000, "[101]", "[201]"))

    def test_secondary_search_updates_only_secondary_list(self) -> None:
        result = self.apply(
            "secondary-search",
            buff_type="secondary",
            expected_value="[201]",
            new_value="[201,202]",
        )
        self.assertEqual(result.status, "applied")
        self.assertEqual(self.row(), (900, 1800, "[101]", "[201,202]"))


if __name__ == "__main__":
    unittest.main()
