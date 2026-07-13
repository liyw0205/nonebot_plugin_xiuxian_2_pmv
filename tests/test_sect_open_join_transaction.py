import tempfile
import unittest
from pathlib import Path

import nonebot

nonebot.init()

from nonebot_plugin_xiuxian_2.xiuxian.xiuxian_sect.open_join_service import (
    SectOpenJoinService,
)
from tests.test_db_backend import db_backend


class SectOpenJoinServiceTests(unittest.TestCase):
    def setUp(self) -> None:
        self.temp_dir = tempfile.TemporaryDirectory()
        self.database = Path(self.temp_dir.name) / "sect.sqlite3"
        with db_backend.transaction(self.database) as conn:
            conn.execute(
                "CREATE TABLE user_xiuxian ("
                "user_id TEXT PRIMARY KEY, sect_id INTEGER, sect_position INTEGER)"
            )
            conn.execute(
                "CREATE TABLE sects ("
                "sect_id INTEGER PRIMARY KEY, sect_name TEXT, sect_owner TEXT, "
                "join_open INTEGER, closed INTEGER)"
            )
            conn.execute("INSERT INTO sects VALUES (1, '青云宗', 'owner', 0, 0)")
            conn.execute("INSERT INTO user_xiuxian VALUES ('owner', 1, 0)")
            conn.execute("INSERT INTO user_xiuxian VALUES ('elder', 1, 2)")
        self.service = SectOpenJoinService(self.database)

    def tearDown(self) -> None:
        self.temp_dir.cleanup()

    def value(self, sql):
        with db_backend.connection(self.database) as conn:
            return conn.execute(sql).fetchone()[0]

    def test_owner_opens_join_and_repeat_is_idempotent(self) -> None:
        first = self.service.open("open-1", "owner", expected_sect_id=1)
        second = self.service.open("open-1", "owner", expected_sect_id=1)

        self.assertEqual(first.status, "opened")
        self.assertEqual(second.status, "duplicate")
        self.assertEqual(self.value("SELECT join_open FROM sects WHERE sect_id=1"), 1)
        self.assertEqual(self.value("SELECT COUNT(*) FROM sect_open_join_operations"), 1)

    def test_open_rechecks_owner_closed_and_current_state(self) -> None:
        self.assertEqual(self.service.open("elder", "elder").status, "not_owner")
        with db_backend.transaction(self.database) as conn:
            conn.execute("UPDATE sects SET closed=1 WHERE sect_id=1")
        self.assertEqual(self.service.open("closed", "owner").status, "sect_closed")
        with db_backend.transaction(self.database) as conn:
            conn.execute("UPDATE sects SET closed=0, join_open=1 WHERE sect_id=1")
        self.assertEqual(self.service.open("already", "owner").status, "already_open")

    def test_failure_rolls_back_state_and_operation(self) -> None:
        with db_backend.transaction(self.database) as conn:
            self.service._ensure_operations(conn)
            conn.execute(
                "CREATE TRIGGER fail_open_operation BEFORE INSERT "
                "ON sect_open_join_operations BEGIN "
                "SELECT RAISE(ABORT, 'operation failed'); END"
            )

        with self.assertRaises(db_backend.IntegrityError):
            self.service.open("fail", "owner")

        self.assertEqual(self.value("SELECT join_open FROM sects WHERE sect_id=1"), 0)

    def test_production_entry_uses_service_without_direct_update(self) -> None:
        source = (
            Path(__file__).parents[1]
            / "nonebot_plugin_xiuxian_2/xiuxian/xiuxian_sect/__init__.py"
        ).read_text(encoding="utf-8")
        start = source.index("async def sect_open_join_")
        end = source.index("@sect_close_mountain.handle", start)
        handler = source[start:end]
        self.assertIn("sect_open_join_service.open(", handler)
        self.assertNotIn("sql_message.update_sect_join_status(", handler)


if __name__ == "__main__":
    unittest.main()
