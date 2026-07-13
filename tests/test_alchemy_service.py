from __future__ import annotations

import tempfile
import unittest
from pathlib import Path

import nonebot

nonebot.init()

from nonebot_plugin_xiuxian_2.xiuxian.xiuxian_back.alchemy_service import AlchemyService
from tests.test_db_backend import db_backend


class AlchemyServiceTests(unittest.TestCase):
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
            conn.execute(
                "INSERT INTO back (user_id,goods_id,goods_num,state,bind_num) VALUES (%s,%s,%s,%s,%s)",
                ("user", 1001, 5, 1, 4),
            )
            conn.execute(
                "INSERT INTO back (user_id,goods_id,goods_num,state,bind_num) VALUES (%s,%s,%s,%s,%s)",
                ("user", 1002, 3, 0, 2),
            )
        self.service = AlchemyService(self.database)

    def tearDown(self) -> None:
        self.temp_dir.cleanup()

    def state(self):
        with db_backend.connection(self.database) as conn:
            stone = conn.execute(
                "SELECT stone FROM user_xiuxian WHERE user_id=%s", ("user",)
            ).fetchone()[0]
            items = conn.execute(
                "SELECT goods_id,goods_num,state,bind_num FROM back WHERE user_id=%s ORDER BY goods_id",
                ("user",),
            ).fetchall()
            exists = conn.execute(
                "SELECT 1 FROM sqlite_master WHERE type='table' AND name=%s",
                ("alchemy_operations",),
            ).fetchone()
            operations = conn.execute("SELECT COUNT(*) FROM alchemy_operations").fetchone()[0] if exists else 0
        return int(stone), [tuple(map(int, row)) for row in items], int(operations)

    def test_batch_consumes_all_items_and_grants_stone_once(self) -> None:
        result = self.service.apply("batch-1", "user", 900, [(1002, 2), (1001, 3)])
        self.assertEqual("applied", result.status)
        self.assertEqual((1000, [(1001, 2, 1, 2), (1002, 1, 0, 1)], 1), self.state())

        duplicate = self.service.apply("batch-1", "user", 900, [(1001, 3), (1002, 2)])
        self.assertEqual("duplicate", duplicate.status)
        self.assertEqual((1000, [(1001, 2, 1, 2), (1002, 1, 0, 1)], 1), self.state())

    def test_operation_conflict_does_not_mutate_state(self) -> None:
        self.service.apply("same", "user", 300, [(1001, 1)])
        before = self.state()
        result = self.service.apply("same", "user", 400, [(1001, 1)])
        self.assertEqual("conflict", result.status)
        self.assertEqual(before, self.state())

    def test_reserved_quantity_and_multi_item_failure_roll_back(self) -> None:
        result = self.service.apply("too-many", "user", 999, [(1001, 5), (1002, 1)])
        self.assertEqual("item_insufficient", result.status)
        self.assertEqual((100, [(1001, 5, 1, 4), (1002, 3, 0, 2)], 0), self.state())

    def test_duplicate_item_ids_are_aggregated_before_validation(self) -> None:
        result = self.service.apply("aggregate", "user", 50, [(1002, 2), (1002, 2)])
        self.assertEqual("item_insufficient", result.status)
        self.assertEqual((100, [(1001, 5, 1, 4), (1002, 3, 0, 2)], 0), self.state())


if __name__ == "__main__":
    unittest.main()
