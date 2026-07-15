from __future__ import annotations

import tempfile
import unittest
from pathlib import Path

import nonebot

nonebot.init()

from nonebot_plugin_xiuxian_2.xiuxian.xiuxian_fusion.fusion_service import FusionService
from tests.test_db_backend import db_backend


class FusionServiceTests(unittest.TestCase):
    def setUp(self) -> None:
        self.temp_dir = tempfile.TemporaryDirectory()
        self.database = Path(self.temp_dir.name) / "fusion.sqlite3"
        with db_backend.transaction(self.database) as conn:
            conn.execute("CREATE TABLE user_xiuxian (user_id TEXT PRIMARY KEY, stone INTEGER NOT NULL)")
            conn.execute("CREATE TABLE back (user_id TEXT NOT NULL, goods_id INTEGER NOT NULL, goods_name TEXT, goods_type TEXT, goods_num INTEGER NOT NULL, bind_num INTEGER DEFAULT 0, UNIQUE(user_id, goods_id))")
            conn.execute("INSERT INTO user_xiuxian VALUES (%s, %s)", ("user", 100))
            conn.execute("INSERT INTO back VALUES (%s, %s, %s, %s, %s, %s)", ("user", 1, "material", "道具", 5, 0))
            conn.execute("INSERT INTO back VALUES (%s, %s, %s, %s, %s, %s)", ("user", 20006, "protection", "道具", 1, 0))
        self.service = FusionService(self.database)

    def tearDown(self) -> None:
        self.temp_dir.cleanup()

    def state(self):
        with db_backend.connection(self.database) as conn:
            stone = int(conn.execute("SELECT stone FROM user_xiuxian WHERE user_id=%s", ("user",)).fetchone()[0])
            items = {int(row[0]): int(row[1]) for row in conn.execute("SELECT goods_id, goods_num FROM back WHERE user_id=%s", ("user",)).fetchall()}
            return stone, items

    def target_binding(self) -> int:
        with db_backend.connection(self.database) as conn:
            row = conn.execute("SELECT bind_num FROM back WHERE user_id=%s AND goods_id=%s", ("user", 2)).fetchone()
            return int(row[0]) if row else 0

    def apply(self, operation_id, successful, protection_item_id=None):
        return self.service.apply(operation_id, "user", 30, {1: 2}, 2, "target", "装备", successful=successful, protection_item_id=protection_item_id, max_goods_num=999)

    def test_success_consumes_all_costs_and_grants_reward(self) -> None:
        result = self.apply("success", True)
        self.assertEqual((result.status, result.successful), ("applied", True))
        self.assertEqual(self.state(), (70, {1: 3, 2: 1, 20006: 1}))
        self.assertEqual(self.target_binding(), 1)

    def test_failed_fusion_consumes_costs_without_reward(self) -> None:
        result = self.apply("failed", False)
        self.assertEqual((result.status, result.protected), ("applied", False))
        self.assertEqual(self.state(), (70, {1: 3, 20006: 1}))

    def test_protection_consumes_only_protection_item(self) -> None:
        result = self.apply("protected", False, 20006)
        self.assertTrue(result.protected)
        self.assertEqual(self.state(), (100, {1: 5, 20006: 0}))

    def test_insufficient_live_material_changes_nothing(self) -> None:
        result = self.service.apply("short", "user", 30, {1: 6}, 2, "target", "装备", successful=True, max_goods_num=999)
        self.assertEqual(result.status, "item_insufficient")
        self.assertEqual(self.state(), (100, {1: 5, 20006: 1}))

    def test_duplicate_reuses_first_roll_and_conflict_is_rejected(self) -> None:
        first = self.apply("repeat", True)
        # mutable successful roll must not break same-op replay
        duplicate = self.apply("repeat", False)
        self.assertEqual((first.status, duplicate.status), ("applied", "duplicate"))
        self.assertTrue(duplicate.successful)
        self.assertIsNotNone(self.service.get_result("repeat"))
        # different target identity conflicts
        conflict = self.service.apply("repeat", "user", 30, {1: 2}, 3, "other", "装备", successful=True, max_goods_num=999)
        self.assertEqual(conflict.status, "state_changed")
        self.assertEqual(self.state(), (70, {1: 3, 2: 1, 20006: 1}))

    def test_operation_failure_rolls_back_every_change(self) -> None:
        with db_backend.transaction(self.database) as conn:
            conn.execute("CREATE TABLE fusion_operations (operation_id TEXT PRIMARY KEY, payload TEXT NOT NULL, successful INTEGER NOT NULL, protected INTEGER NOT NULL)")
            conn.execute("CREATE TRIGGER fail_fusion BEFORE INSERT ON fusion_operations BEGIN SELECT RAISE(ABORT, 'failed'); END")
        with self.assertRaises(db_backend.IntegrityError):
            self.apply("rollback", True)
        self.assertEqual(self.state(), (100, {1: 5, 20006: 1}))


if __name__ == "__main__":
    unittest.main()
