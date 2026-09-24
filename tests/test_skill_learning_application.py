from __future__ import annotations

import sqlite3
import tempfile
import unittest
from pathlib import Path

from nonebot_plugin_xiuxian_2.features.back.migrations import apply_skill_learning
from nonebot_plugin_xiuxian_2.features.back.skill_learning_application import SkillLearningApplication
from nonebot_plugin_xiuxian_2.infrastructure.database import DatabaseUnitOfWork
from tests.test_db_backend import db_backend


class SkillLearningApplicationTests(unittest.TestCase):
    def setUp(self) -> None:
        self.temp_dir = tempfile.TemporaryDirectory()
        self.database = Path(self.temp_dir.name) / "skill-learning.sqlite3"
        with db_backend.transaction(self.database) as conn:
            conn.execute(
                "CREATE TABLE back (user_id TEXT, goods_id INTEGER, goods_num INTEGER, "
                "bind_num INTEGER, update_time TEXT, action_time TEXT, UNIQUE(user_id,goods_id))"
            )
            conn.execute(
                "CREATE TABLE BuffInfo (user_id TEXT PRIMARY KEY, main_buff INTEGER, sub_buff INTEGER, "
                "sec_buff INTEGER, effect1_buff INTEGER, effect2_buff INTEGER)"
            )
            conn.execute("INSERT INTO back VALUES (%s,%s,%s,%s,NULL,NULL)", ("user", 8001, 2, 1))
            conn.execute("INSERT INTO back VALUES (%s,%s,%s,%s,NULL,NULL)", ("user", 8002, 1, 1))
            conn.execute("INSERT INTO BuffInfo VALUES (%s,0,7001,0,0,0)", ("user",))
        with DatabaseUnitOfWork(self.database) as uow:
            apply_skill_learning(uow)
        self.application = SkillLearningApplication(self.database)

    def tearDown(self) -> None:
        self.temp_dir.cleanup()

    def _state(self):
        with db_backend.connection(self.database) as conn:
            inventory = conn.execute(
                "SELECT goods_id,goods_num,bind_num FROM back ORDER BY goods_id"
            ).fetchall()
            buff = conn.execute(
                "SELECT main_buff,sub_buff,sec_buff,effect1_buff,effect2_buff FROM BuffInfo WHERE user_id=%s",
                ("user",),
            ).fetchone()
            count = conn.execute("SELECT COUNT(*) FROM skill_learning_operations").fetchone()[0]
        return [tuple(map(int, row)) for row in inventory], tuple(map(int, buff)), int(count)

    def test_success_duplicate_and_replacement(self) -> None:
        first = self.application.learn("learn-1", "user", 8001, "功法")
        duplicate = self.application.learn("learn-1", "user", 8001, "功法")
        replacement = self.application.learn("learn-sub", "user", 8002, "辅修功法")
        self.assertEqual((first.status, duplicate.status, replacement.status), ("learned", "duplicate", "learned"))
        self.assertEqual((first.previous_item_id, replacement.previous_item_id), (0, 7001))
        self.assertEqual(([(8001, 1, 0), (8002, 0, 0)], (8001, 8002, 0, 0, 0), 2), self._state())

    def test_missing_or_already_learned_does_not_mutate(self) -> None:
        missing = self.application.learn("learn-missing", "user", 9999, "身法")
        already = self.application.learn("learn-existing", "user", 8001, "神通")
        with db_backend.transaction(self.database) as conn:
            conn.execute("UPDATE BuffInfo SET effect2_buff=%s WHERE user_id=%s", (8001, "user"))
        same = self.application.learn("learn-same", "user", 8001, "瞳术")
        self.assertEqual((missing.status, already.status, same.status), ("item_missing", "learned", "already_learned"))
        self.assertEqual(([(8001, 1, 0), (8002, 1, 1)], (0, 7001, 8001, 0, 8001), 1), self._state())

    def test_missing_schema_is_not_created_at_request_time(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            database = Path(directory) / "empty.sqlite3"
            with DatabaseUnitOfWork(database) as uow:
                uow.execute("CREATE TABLE back(user_id TEXT, goods_id INTEGER, goods_num INTEGER)")
                uow.execute("CREATE TABLE BuffInfo(user_id TEXT PRIMARY KEY, main_buff INTEGER)")
            with self.assertRaises(sqlite3.OperationalError):
                SkillLearningApplication(database).learn("missing-schema", "user", 1, "功法")


if __name__ == "__main__":
    unittest.main()
