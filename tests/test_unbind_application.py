from __future__ import annotations

import sqlite3
import tempfile
import unittest
from pathlib import Path

from nonebot_plugin_xiuxian_2.features.back.migrations import apply_unbind
from nonebot_plugin_xiuxian_2.features.back.unbind_application import UnbindApplication
from nonebot_plugin_xiuxian_2.infrastructure.database import DatabaseUnitOfWork
from tests.test_db_backend import db_backend


class UnbindApplicationTests(unittest.TestCase):
    def setUp(self) -> None:
        self.temp_dir = tempfile.TemporaryDirectory()
        self.database = Path(self.temp_dir.name) / "unbind.sqlite3"
        with db_backend.transaction(self.database) as conn:
            conn.execute(
                "CREATE TABLE back (user_id TEXT, goods_id INTEGER, goods_num INTEGER, "
                "bind_num INTEGER, update_time TEXT, action_time TEXT, UNIQUE(user_id,goods_id))"
            )
            conn.execute("INSERT INTO back VALUES (%s,%s,%s,%s,NULL,NULL)", ("user", 20019, 3, 2))
            conn.execute("INSERT INTO back VALUES (%s,%s,%s,%s,NULL,NULL)", ("user", 9001, 5, 4))
        with DatabaseUnitOfWork(self.database) as uow:
            apply_unbind(uow)
        self.application = UnbindApplication(self.database)

    def tearDown(self) -> None:
        self.temp_dir.cleanup()

    def _state(self):
        with db_backend.connection(self.database) as conn:
            rows = conn.execute(
                "SELECT goods_id,goods_num,bind_num FROM back ORDER BY goods_id"
            ).fetchall()
            count = conn.execute("SELECT COUNT(*) FROM unbind_item_operations").fetchone()[0]
        return [tuple(map(int, row)) for row in rows], int(count)

    def test_success_duplicate_and_quantity_cap(self) -> None:
        first = self.application.apply("unbind-1", "user", 20019, 9001, 10)
        duplicate = self.application.apply("unbind-1", "user", 20019, 9001, 1)
        self.assertEqual((first.status, first.quantity, duplicate.status, duplicate.quantity), ("applied", 3, "duplicate", 3))
        self.assertEqual(([(9001, 5, 1), (20019, 0, 0)], 1), self._state())

    def test_missing_or_unbound_target_does_not_consume_charm(self) -> None:
        result = self.application.apply("unbind-none", "user", 20019, 9999, 1)
        self.assertEqual(result.status, "target_missing")
        self.assertEqual(([(9001, 5, 4), (20019, 3, 2)], 0), self._state())

    def test_missing_schema_is_not_created_at_request_time(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            database = Path(directory) / "empty.sqlite3"
            with DatabaseUnitOfWork(database) as uow:
                uow.execute("CREATE TABLE back(user_id TEXT, goods_id INTEGER, goods_num INTEGER, bind_num INTEGER)")
            with self.assertRaises(sqlite3.OperationalError):
                UnbindApplication(database).apply("missing-schema", "u", 20019, 9001, 1)


if __name__ == "__main__":
    unittest.main()
