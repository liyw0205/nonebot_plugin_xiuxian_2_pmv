from __future__ import annotations

import tempfile
import unittest
import sqlite3
from pathlib import Path

from nonebot_plugin_xiuxian_2.features.back.alchemy_application import AlchemyApplication
from nonebot_plugin_xiuxian_2.features.back.migrations import apply_alchemy
from nonebot_plugin_xiuxian_2.infrastructure.database import DatabaseUnitOfWork
from tests.test_db_backend import db_backend


class AlchemyApplicationTests(unittest.TestCase):
    def setUp(self) -> None:
        self.temp_dir = tempfile.TemporaryDirectory()
        self.database = Path(self.temp_dir.name) / "alchemy.sqlite3"
        with db_backend.transaction(self.database) as conn:
            conn.execute("CREATE TABLE user_xiuxian (user_id TEXT PRIMARY KEY, stone INTEGER)")
            conn.execute(
                "CREATE TABLE back (user_id TEXT, goods_id INTEGER, goods_num INTEGER, "
                "state INTEGER DEFAULT 0, bind_num INTEGER DEFAULT 0, update_time TEXT, "
                "action_time TEXT, UNIQUE(user_id,goods_id))"
            )
            conn.execute("INSERT INTO user_xiuxian VALUES (%s,%s)", ("user", 100))
            conn.execute("INSERT INTO back VALUES (%s,%s,%s,%s,%s,NULL,NULL)", ("user", 1001, 5, 1, 4))
            conn.execute("INSERT INTO back VALUES (%s,%s,%s,%s,%s,NULL,NULL)", ("user", 1002, 3, 0, 2))
        with DatabaseUnitOfWork(self.database) as uow:
            apply_alchemy(uow)
        self.application = AlchemyApplication(self.database)

    def tearDown(self) -> None:
        self.temp_dir.cleanup()

    def _state(self):
        with db_backend.connection(self.database) as conn:
            stone = conn.execute("SELECT stone FROM user_xiuxian WHERE user_id=%s", ("user",)).fetchone()[0]
            items = conn.execute(
                "SELECT goods_id,goods_num,state,bind_num FROM back WHERE user_id=%s ORDER BY goods_id",
                ("user",),
            ).fetchall()
            operations = conn.execute("SELECT COUNT(*) FROM alchemy_operations").fetchone()[0]
        return int(stone), [tuple(map(int, row)) for row in items], int(operations)

    def test_success_duplicate_and_payload_conflict(self) -> None:
        first = self.application.apply("alchemy-1", "user", 900, [(1002, 2), (1001, 3)])
        duplicate = self.application.apply("alchemy-1", "user", 900, [(1001, 3), (1002, 2)])
        conflict = self.application.apply("alchemy-1", "user", 901, [(1001, 3), (1002, 2)])
        self.assertEqual((first.status, duplicate.status, conflict.status), ("applied", "duplicate", "operation_conflict"))
        self.assertEqual((1000, [(1001, 2, 1, 2), (1002, 1, 0, 1)], 1), self._state())

    def test_reserved_quantity_and_multi_item_failure_roll_back(self) -> None:
        result = self.application.apply("alchemy-fail", "user", 999, [(1001, 5), (1002, 1)])
        self.assertEqual(result.status, "item_insufficient")
        self.assertEqual((100, [(1001, 5, 1, 4), (1002, 3, 0, 2)], 0), self._state())

    def test_duplicate_item_ids_are_aggregated_before_validation(self) -> None:
        result = self.application.apply("alchemy-aggregate", "user", 50, [(1002, 2), (1002, 2)])
        self.assertEqual(result.status, "item_insufficient")
        self.assertEqual((100, [(1001, 5, 1, 4), (1002, 3, 0, 2)], 0), self._state())

    def test_missing_schema_is_not_created_at_request_time(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            database = Path(directory) / "empty.sqlite3"
            with DatabaseUnitOfWork(database) as uow:
                uow.execute("CREATE TABLE user_xiuxian(user_id TEXT PRIMARY KEY, stone INTEGER)")
                uow.execute("CREATE TABLE back(user_id TEXT, goods_id INTEGER, goods_num INTEGER, state INTEGER)")
                uow.execute("INSERT INTO user_xiuxian VALUES('u', 0)")
            with self.assertRaises(sqlite3.OperationalError):
                AlchemyApplication(database).apply("missing-schema", "u", 1, [(1, 1)])


if __name__ == "__main__":
    unittest.main()
