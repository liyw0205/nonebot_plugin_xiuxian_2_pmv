from __future__ import annotations

import tempfile
import unittest
from pathlib import Path

import nonebot

nonebot.init()

from nonebot_plugin_xiuxian_2.xiuxian.xiuxian_back.skill_learning_service import (
    SkillLearningService,
)
from tests.test_db_backend import db_backend


class SkillLearningServiceTests(unittest.TestCase):
    def setUp(self) -> None:
        self.temp_dir = tempfile.TemporaryDirectory()
        self.database = Path(self.temp_dir.name) / "skill-learning.sqlite3"
        with db_backend.transaction(self.database) as conn:
            conn.execute(
                "CREATE TABLE back (user_id TEXT, goods_id INTEGER, goods_num INTEGER, "
                "bind_num INTEGER, UNIQUE(user_id, goods_id))"
            )
            conn.execute(
                "CREATE TABLE BuffInfo (user_id TEXT PRIMARY KEY, main_buff INTEGER, "
                "sub_buff INTEGER, sec_buff INTEGER, effect1_buff INTEGER, "
                "effect2_buff INTEGER)"
            )
            conn.execute("INSERT INTO back VALUES (%s, %s, %s, %s)", ("user", 8001, 2, 1))
            conn.execute("INSERT INTO back VALUES (%s, %s, %s, %s)", ("user", 8002, 1, 1))
            conn.execute("INSERT INTO BuffInfo VALUES (%s, 0, 7001, 0, 0, 0)", ("user",))
        self.service = SkillLearningService(self.database)

    def tearDown(self) -> None:
        self.temp_dir.cleanup()

    def inventory(self, goods_id: int):
        with db_backend.connection(self.database) as conn:
            row = conn.execute(
                "SELECT goods_num, bind_num FROM back WHERE user_id=%s AND goods_id=%s",
                ("user", goods_id),
            ).fetchone()
        return tuple(map(int, row)) if row else None

    def buff(self, column: str) -> int:
        with db_backend.connection(self.database) as conn:
            return int(
                conn.execute(f"SELECT {column} FROM BuffInfo WHERE user_id=%s", ("user",)).fetchone()[0]
            )

    def operation_count(self) -> int:
        with db_backend.connection(self.database) as conn:
            exists = conn.execute(
                "SELECT 1 FROM sqlite_master WHERE type='table' AND name=%s",
                ("skill_learning_operations",),
            ).fetchone()
            if not exists:
                return 0
            return int(conn.execute("SELECT COUNT(*) FROM skill_learning_operations").fetchone()[0])

    def test_learning_consumes_book_and_updates_matching_slot_atomically(self) -> None:
        result = self.service.learn("learn-1", "user", 8001, "功法")
        self.assertEqual((result.status, result.previous_item_id), ("learned", 0))
        self.assertEqual(self.inventory(8001), (1, 0))
        self.assertEqual(self.buff("main_buff"), 8001)
        self.assertEqual(self.operation_count(), 1)

    def test_learning_replaces_previous_skill_and_consumes_one_book(self) -> None:
        result = self.service.learn("learn-sub", "user", 8002, "辅修功法")
        self.assertEqual((result.status, result.previous_item_id), ("learned", 7001))
        self.assertEqual(self.inventory(8002), (0, 0))
        self.assertEqual(self.buff("sub_buff"), 8002)

    def test_duplicate_event_does_not_consume_second_book(self) -> None:
        first = self.service.learn("learn-repeat", "user", 8001, "神通")
        second = self.service.learn("learn-repeat", "user", 8001, "神通")
        self.assertEqual((first.status, second.status), ("learned", "duplicate"))
        self.assertEqual(self.inventory(8001), (1, 0))
        self.assertEqual(self.buff("sec_buff"), 8001)
        self.assertEqual(self.operation_count(), 1)

    def test_missing_or_already_learned_book_does_not_mutate_state(self) -> None:
        missing = self.service.learn("learn-missing", "user", 9999, "身法")
        with db_backend.transaction(self.database) as conn:
            conn.execute("UPDATE BuffInfo SET effect2_buff=%s WHERE user_id=%s", (8001, "user"))
        learned = self.service.learn("learn-existing", "user", 8001, "瞳术")
        self.assertEqual(missing.status, "item_missing")
        self.assertEqual(learned.status, "already_learned")
        self.assertEqual(self.inventory(8001), (2, 1))
        self.assertEqual(self.operation_count(), 0)

    def test_operation_failure_rolls_back_book_and_skill_slot(self) -> None:
        with db_backend.transaction(self.database) as conn:
            self.service._ensure_operations(conn)
            conn.execute(
                "CREATE TRIGGER fail_skill_learning BEFORE INSERT ON skill_learning_operations "
                "BEGIN SELECT RAISE(ABORT, 'operation failed'); END"
            )
        with self.assertRaises(db_backend.IntegrityError):
            self.service.learn("learn-fail", "user", 8001, "功法")
        self.assertEqual(self.inventory(8001), (2, 1))
        self.assertEqual(self.buff("main_buff"), 0)
        self.assertEqual(self.operation_count(), 0)

    def test_rejects_unknown_skill_type(self) -> None:
        with self.assertRaises(ValueError):
            self.service.learn("learn-invalid", "user", 8001, "阵法")


if __name__ == "__main__":
    unittest.main()
