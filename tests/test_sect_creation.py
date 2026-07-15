from __future__ import annotations

import tempfile
import unittest
from pathlib import Path

import nonebot

nonebot.init()

from nonebot_plugin_xiuxian_2.xiuxian.xiuxian_sect.transaction_service import SectMembershipService
from tests.test_db_backend import db_backend


class SectCreationTests(unittest.TestCase):
    def setUp(self) -> None:
        self.temp_dir = tempfile.TemporaryDirectory()
        self.database = Path(self.temp_dir.name) / "sect.sqlite3"
        with db_backend.transaction(self.database) as conn:
            conn.execute("CREATE TABLE user_xiuxian (user_id TEXT PRIMARY KEY, sect_id INTEGER, sect_position INTEGER, stone INTEGER)")
            conn.execute("CREATE TABLE sects (sect_id INTEGER PRIMARY KEY, sect_name TEXT NOT NULL, sect_owner TEXT, sect_scale INTEGER, sect_used_stone INTEGER, join_open INTEGER, closed INTEGER, combat_power INTEGER)")
            conn.execute("INSERT INTO user_xiuxian VALUES (%s, %s, %s, %s)", ("user", None, None, 1000))
            conn.execute("INSERT INTO sects VALUES (%s, %s, %s, %s, %s, %s, %s, %s)", (1, "已有宗门", "owner", 0, 0, 1, 0, 0))
        self.service = SectMembershipService(self.database)

    def tearDown(self) -> None:
        self.temp_dir.cleanup()

    def user_state(self):
        with db_backend.connection(self.database) as conn:
            return tuple(conn.execute("SELECT sect_id, sect_position, stone FROM user_xiuxian WHERE user_id=%s", ("user",)).fetchone())

    def test_creation_charges_and_assigns_owner_atomically(self) -> None:
        result = self.service.create_sect("create-1", "user", "青云宗", 300, 0)
        self.assertEqual(result.status, "created")
        self.assertEqual(self.user_state(), (result.sect_id, 0, 700))
        with db_backend.connection(self.database) as conn:
            sect = conn.execute("SELECT sect_name, sect_owner FROM sects WHERE sect_id=%s", (result.sect_id,)).fetchone()
        self.assertEqual(tuple(sect), ("青云宗", "user"))

    def test_duplicate_does_not_create_or_charge_twice(self) -> None:
        first = self.service.create_sect("create-repeat", "user", "青云宗", 300, 0)
        second = self.service.create_sect("create-repeat", "user", "其他宗门", 999, 9)
        self.assertEqual(second.status, "duplicate")
        self.assertEqual(second.sect_id, first.sect_id)
        self.assertEqual(self.user_state(), (first.sect_id, 0, 700))
        with db_backend.connection(self.database) as conn:
            self.assertEqual(conn.execute("SELECT COUNT(*) FROM sects").fetchone()[0], 2)

    def test_name_conflict_and_insufficient_stones_do_not_mutate(self) -> None:
        self.assertEqual(self.service.create_sect("name", "user", "已有宗门", 300, 0).status, "name_exists")
        self.assertEqual(self.service.create_sect("poor", "user", "青云宗", 1001, 0).status, "stone_insufficient")
        self.assertEqual(self.user_state(), (None, None, 1000))

    def test_creation_failure_rolls_back_insert_and_charge(self) -> None:
        with db_backend.transaction(self.database) as conn:
            self.service._ensure_creation_operations(conn)
            conn.execute("CREATE TRIGGER fail_creation BEFORE INSERT ON sect_creation_operations BEGIN SELECT RAISE(ABORT, 'creation failed'); END")
        with self.assertRaises(db_backend.IntegrityError):
            self.service.create_sect("create-fail", "user", "青云宗", 300, 0)
        self.assertEqual(self.user_state(), (None, None, 1000))
        with db_backend.connection(self.database) as conn:
            self.assertEqual(conn.execute("SELECT COUNT(*) FROM sects").fetchone()[0], 1)

    def test_refresh_charge_is_idempotent_and_checks_current_state(self) -> None:
        first = self.service.charge_name_refresh("refresh-1", "user", 100)
        second = self.service.charge_name_refresh("refresh-1", "user", 999)
        self.assertEqual((first.status, second.status), ("charged", "duplicate"))
        self.assertEqual(self.user_state(), (None, None, 900))
        self.assertEqual(self.service.charge_name_refresh("refresh-poor", "user", 901).status, "stone_insufficient")
        self.assertEqual(self.user_state(), (None, None, 900))


if __name__ == "__main__":
    unittest.main()
