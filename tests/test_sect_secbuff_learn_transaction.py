from __future__ import annotations

import tempfile
import unittest
from pathlib import Path

import nonebot

nonebot.init()

from nonebot_plugin_xiuxian_2.xiuxian.xiuxian_sect.transaction_service import (
    SectSecBuffLearnService,
)
from tests.test_db_backend import db_backend


class SectSecBuffLearnServiceTests(unittest.TestCase):
    def setUp(self) -> None:
        self.temp_dir = tempfile.TemporaryDirectory()
        self.database = Path(self.temp_dir.name) / "game.sqlite3"
        with db_backend.transaction(self.database) as conn:
            conn.execute(
                "CREATE TABLE user_xiuxian (user_id TEXT PRIMARY KEY, sect_id INTEGER, sect_position INTEGER)"
            )
            conn.execute(
                "CREATE TABLE sects (sect_id INTEGER PRIMARY KEY, secbuff TEXT, sect_materials INTEGER)"
            )
            conn.execute("CREATE TABLE BuffInfo (user_id TEXT PRIMARY KEY, sec_buff INTEGER)")
            conn.execute("INSERT INTO user_xiuxian VALUES (%s, %s, %s)", ("user", 1, 3))
            conn.execute("INSERT INTO sects VALUES (%s, %s, %s)", (1, "[2001,2002]", 500))
            conn.execute("INSERT INTO BuffInfo VALUES (%s, %s)", ("user", 1900))
        self.service = SectSecBuffLearnService(self.database)

    def tearDown(self) -> None:
        self.temp_dir.cleanup()

    def row(self, sql, params=()):
        with db_backend.connection(self.database) as conn:
            return conn.execute(sql, params).fetchone()

    def test_learning_deducts_materials_and_updates_buff_atomically(self) -> None:
        result = self.service.learn(
            "sec-1", "user", 1, 2001, 150, expected_catalog="[2001,2002]"
        )

        self.assertEqual(result.status, "learned")
        self.assertEqual(self.row("SELECT sect_materials FROM sects WHERE sect_id=1")[0], 350)
        self.assertEqual(self.row("SELECT sec_buff FROM BuffInfo WHERE user_id='user'")[0], 2001)

    def test_duplicate_operation_does_not_charge_twice(self) -> None:
        first = self.service.learn("sec-repeat", "user", 1, 2001, 150, expected_catalog="[2001,2002]")
        second = self.service.learn("sec-repeat", "user", 1, 2001, 150, expected_catalog="[2001,2002]")

        self.assertEqual(first.status, "learned")
        self.assertEqual(second.status, "duplicate")
        self.assertEqual(self.row("SELECT sect_materials FROM sects WHERE sect_id=1")[0], 350)

    def test_forbidden_position_is_rejected(self) -> None:
        with db_backend.transaction(self.database) as conn:
            conn.execute("UPDATE user_xiuxian SET sect_position=12 WHERE user_id=%s", ("user",))

        result = self.service.learn("sec-position", "user", 1, 2001, 150, expected_catalog="[2001,2002]")

        self.assertEqual(result.status, "position_forbidden")
        self.assertEqual(self.row("SELECT sect_materials FROM sects WHERE sect_id=1")[0], 500)
        self.assertEqual(self.row("SELECT sec_buff FROM BuffInfo WHERE user_id='user'")[0], 1900)

    def test_buff_update_failure_rolls_back_materials(self) -> None:
        with db_backend.transaction(self.database) as conn:
            conn.execute(
                "CREATE TRIGGER fail_secbuff BEFORE UPDATE ON BuffInfo "
                "BEGIN SELECT RAISE(ABORT, 'buff failed'); END"
            )

        with self.assertRaises(db_backend.IntegrityError):
            self.service.learn("sec-fail", "user", 1, 2001, 150, expected_catalog="[2001,2002]")

        self.assertEqual(self.row("SELECT sect_materials FROM sects WHERE sect_id=1")[0], 500)
        self.assertEqual(self.row("SELECT sec_buff FROM BuffInfo WHERE user_id='user'")[0], 1900)

    def test_handler_uses_service_without_old_writes(self) -> None:
        source = (
            Path(__file__).parents[1]
            / "nonebot_plugin_xiuxian_2/xiuxian/xiuxian_sect/__init__.py"
        ).read_text(encoding="utf-8")
        start = source.index("async def sect_secbuff_learn_")
        end = source.index("@upatkpractice.handle", start)
        handler = source[start:end]

        self.assertIn("sect_secbuff_learn_service.learn(", handler)
        self.assertNotIn("sql_message.update_sect_materials(", handler)
        self.assertNotIn("sql_message.updata_user_sec_buff(", handler)


if __name__ == "__main__":
    unittest.main()
