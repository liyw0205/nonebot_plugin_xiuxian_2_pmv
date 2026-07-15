import tempfile
import unittest
from pathlib import Path

import nonebot

nonebot.init()

from nonebot_plugin_xiuxian_2.xiuxian.xiuxian_sect.transaction_service import (
    SectMemberJoinService,
)
from tests.test_db_backend import db_backend


class SectMemberJoinServiceTests(unittest.TestCase):
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
                "sect_id INTEGER PRIMARY KEY, sect_name TEXT, sect_scale INTEGER, "
                "join_open INTEGER, closed INTEGER)"
            )
            conn.execute("INSERT INTO sects VALUES (1, '青云宗', 0, 1, 0)")
            conn.execute("INSERT INTO user_xiuxian VALUES ('new', NULL, NULL)")
            for index in range(19):
                conn.execute(
                    "INSERT INTO user_xiuxian VALUES (%s, 1, 12)", (f"member-{index}",)
                )
        self.service = SectMemberJoinService(self.database)

    def tearDown(self) -> None:
        self.temp_dir.cleanup()

    def row(self, sql, params=()):
        with db_backend.connection(self.database) as conn:
            return conn.execute(sql, params).fetchone()

    def test_join_writes_member_at_live_capacity_boundary(self) -> None:
        result = self.service.join("join-1", "new", 1, member_position=12)

        self.assertEqual(result.status, "joined")
        self.assertEqual((result.member_count, result.member_limit), (20, 20))
        self.assertEqual(
            tuple(self.row("SELECT sect_id, sect_position FROM user_xiuxian WHERE user_id='new'")),
            (1, 12),
        )
        self.assertEqual(self.service.join("join-1", "new", 1).status, "duplicate")

    def test_join_rechecks_open_closed_and_membership(self) -> None:
        with db_backend.transaction(self.database) as conn:
            conn.execute("UPDATE sects SET join_open=0 WHERE sect_id=1")
        self.assertEqual(self.service.join("closed-join", "new", 1).status, "join_closed")
        with db_backend.transaction(self.database) as conn:
            conn.execute("UPDATE sects SET join_open=1, closed=1 WHERE sect_id=1")
        self.assertEqual(self.service.join("closed-sect", "new", 1).status, "sect_closed")
        with db_backend.transaction(self.database) as conn:
            conn.execute("UPDATE sects SET closed=0")
            conn.execute("UPDATE user_xiuxian SET sect_id=1 WHERE user_id='new'")
        self.assertEqual(self.service.join("member", "new", 1).status, "already_in_sect")

    def test_full_sect_does_not_write_member(self) -> None:
        with db_backend.transaction(self.database) as conn:
            conn.execute("INSERT INTO user_xiuxian VALUES ('last', 1, 12)")

        result = self.service.join("full", "new", 1)

        self.assertEqual(result.status, "sect_full")
        self.assertEqual((result.member_count, result.member_limit), (20, 20))
        self.assertIsNone(self.row("SELECT sect_id FROM user_xiuxian WHERE user_id='new'")[0])

    def test_operation_failure_rolls_back_membership(self) -> None:
        with db_backend.transaction(self.database) as conn:
            self.service._ensure_operations(conn)
            conn.execute(
                "CREATE TRIGGER fail_join_operation BEFORE INSERT "
                "ON sect_member_join_operations BEGIN "
                "SELECT RAISE(ABORT, 'operation failed'); END"
            )

        with self.assertRaises(db_backend.IntegrityError):
            self.service.join("fail", "new", 1)

        self.assertIsNone(self.row("SELECT sect_id FROM user_xiuxian WHERE user_id='new'")[0])

    def test_production_entry_uses_service_without_split_write(self) -> None:
        source = (
            Path(__file__).parents[1]
            / "nonebot_plugin_xiuxian_2/xiuxian/xiuxian_sect/__init__.py"
        ).read_text(encoding="utf-8")
        start = source.index("async def join_sect_")
        end = source.index("@my_sect.handle", start)
        handler = source[start:end]
        self.assertIn("sect_member_join_service.join(", handler)
        self.assertNotIn("sql_message.update_usr_sect(", handler)
        self.assertNotIn("can_join_sect(", handler)


if __name__ == "__main__":
    unittest.main()
