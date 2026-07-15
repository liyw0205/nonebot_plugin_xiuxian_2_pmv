from __future__ import annotations

import tempfile
import unittest
from pathlib import Path

import nonebot

nonebot.init()

from nonebot_plugin_xiuxian_2.xiuxian.xiuxian_sect.transaction_service import (
    SectMainBuffLearnService,
)
from tests.test_db_backend import db_backend


class SectMainBuffLearnServiceTests(unittest.TestCase):
    def setUp(self) -> None:
        self.temp_dir = tempfile.TemporaryDirectory()
        self.database = Path(self.temp_dir.name) / "game.sqlite3"
        with db_backend.transaction(self.database) as conn:
            conn.execute(
                "CREATE TABLE user_xiuxian (user_id TEXT PRIMARY KEY, sect_id INTEGER, sect_position INTEGER)"
            )
            conn.execute(
                "CREATE TABLE sects (sect_id INTEGER PRIMARY KEY, mainbuff TEXT, sect_materials INTEGER)"
            )
            conn.execute(
                "CREATE TABLE BuffInfo (user_id TEXT PRIMARY KEY, main_buff INTEGER)"
            )
            conn.execute("INSERT INTO user_xiuxian VALUES (%s, %s, %s)", ("user", 1, 3))
            conn.execute("INSERT INTO sects VALUES (%s, %s, %s)", (1, "[1001,1002]", 500))
            conn.execute("INSERT INTO BuffInfo VALUES (%s, %s)", ("user", 900))
        self.service = SectMainBuffLearnService(self.database)

    def tearDown(self) -> None:
        self.temp_dir.cleanup()

    def row(self, sql, params=()):
        with db_backend.connection(self.database) as conn:
            return conn.execute(sql, params).fetchone()

    def test_learning_deducts_materials_and_updates_buff_atomically(self) -> None:
        result = self.service.learn(
            "main-1", "user", 1, 1001, 120, expected_catalog="[1001,1002]"
        )

        self.assertEqual(result.status, "learned")
        self.assertEqual(self.row("SELECT sect_materials FROM sects WHERE sect_id=1")[0], 380)
        self.assertEqual(self.row("SELECT main_buff FROM BuffInfo WHERE user_id='user'")[0], 1001)

    def test_duplicate_operation_does_not_charge_twice(self) -> None:
        first = self.service.learn("main-repeat", "user", 1, 1001, 120, expected_catalog="[1001,1002]")
        second = self.service.learn("main-repeat", "user", 1, 1001, 120, expected_catalog="[1001,1002]")

        self.assertEqual(first.status, "learned")
        self.assertEqual(second.status, "duplicate")
        self.assertEqual(self.row("SELECT sect_materials FROM sects WHERE sect_id=1")[0], 380)

    def test_stale_catalog_or_membership_is_rejected(self) -> None:
        with db_backend.transaction(self.database) as conn:
            conn.execute("UPDATE sects SET mainbuff=%s WHERE sect_id=1", ("[1002]",))

        result = self.service.learn("main-stale", "user", 1, 1001, 120, expected_catalog="[1001,1002]")

        self.assertEqual(result.status, "catalog_changed")
        self.assertEqual(self.row("SELECT sect_materials FROM sects WHERE sect_id=1")[0], 500)
        self.assertEqual(self.row("SELECT main_buff FROM BuffInfo WHERE user_id='user'")[0], 900)

    def test_buff_update_failure_rolls_back_materials(self) -> None:
        with db_backend.transaction(self.database) as conn:
            conn.execute(
                "CREATE TRIGGER fail_mainbuff BEFORE UPDATE ON BuffInfo "
                "BEGIN SELECT RAISE(ABORT, 'buff failed'); END"
            )

        with self.assertRaises(db_backend.IntegrityError):
            self.service.learn("main-fail", "user", 1, 1001, 120, expected_catalog="[1001,1002]")

        self.assertEqual(self.row("SELECT sect_materials FROM sects WHERE sect_id=1")[0], 500)
        self.assertEqual(self.row("SELECT main_buff FROM BuffInfo WHERE user_id='user'")[0], 900)

    def test_handler_uses_service_without_old_writes(self) -> None:
        source = (
            Path(__file__).parents[1]
            / "nonebot_plugin_xiuxian_2/xiuxian/xiuxian_sect/__init__.py"
        ).read_text(encoding="utf-8")
        start = source.index("async def sect_mainbuff_learn_")
        end = source.index("@sect_mainbuff_get.handle", start)
        handler = source[start:end]

        self.assertIn("sect_mainbuff_learn_service.learn(", handler)
        self.assertNotIn("sql_message.update_sect_materials(", handler)
        self.assertNotIn("sql_message.updata_user_main_buff(", handler)


if __name__ == "__main__":
    unittest.main()
