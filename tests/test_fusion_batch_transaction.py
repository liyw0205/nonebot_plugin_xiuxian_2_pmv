import tempfile
import unittest
from pathlib import Path

import nonebot

nonebot.init()

from nonebot_plugin_xiuxian_2.xiuxian.xiuxian_fusion import parse_fusion_args
from nonebot_plugin_xiuxian_2.xiuxian.xiuxian_fusion.fusion_service import FusionService
from tests.test_db_backend import db_backend


class FusionBatchTransactionTests(unittest.TestCase):
    def setUp(self) -> None:
        self.temp_dir = tempfile.TemporaryDirectory()
        self.database = Path(self.temp_dir.name) / "fusion-batch.sqlite3"
        with db_backend.transaction(self.database) as conn:
            conn.execute("CREATE TABLE user_xiuxian (user_id TEXT PRIMARY KEY, stone INTEGER NOT NULL)")
            conn.execute("CREATE TABLE back (user_id TEXT NOT NULL, goods_id INTEGER NOT NULL, goods_name TEXT, goods_type TEXT, goods_num INTEGER NOT NULL, bind_num INTEGER DEFAULT 0, UNIQUE(user_id, goods_id))")
            conn.execute("INSERT INTO user_xiuxian VALUES (%s, %s)", ("user", 200))
            conn.execute("INSERT INTO back VALUES (%s, %s, %s, %s, %s, %s)", ("user", 1, "material", "装备", 10, 1))
            conn.execute("INSERT INTO back VALUES (%s, %s, %s, %s, %s, %s)", ("user", 20006, "protection", "道具", 2, 0))
        self.service = FusionService(self.database)

    def tearDown(self) -> None:
        self.temp_dir.cleanup()

    def state(self):
        with db_backend.connection(self.database) as conn:
            stone = int(conn.execute("SELECT stone FROM user_xiuxian WHERE user_id=%s", ("user",)).fetchone()[0])
            inventory = {int(row[0]): (int(row[1]), int(row[2])) for row in conn.execute("SELECT goods_id, goods_num, bind_num FROM back WHERE user_id=%s", ("user",)).fetchall()}
            return stone, inventory

    def apply(self, operation_id="batch", outcomes=(True, False, False, False), **kwargs):
        return self.service.apply_batch(operation_id, "user", 30, {1: 2}, 2, "target", "装备", outcomes, protection_item_id=20006, reserved_items={1: 1}, max_goods_num=999, **kwargs)

    def test_command_accepts_optional_positive_quantity(self) -> None:
        self.assertEqual(parse_fusion_args("青锋剑 10"), ("青锋剑", 10))
        self.assertEqual(parse_fusion_args("青锋剑"), ("青锋剑", 1))
        self.assertEqual(parse_fusion_args("青锋剑 0"), ("青锋剑", None))

    def test_partial_results_settle_all_costs_and_rewards_once(self) -> None:
        result = self.apply()
        self.assertEqual((result.status, result.successful_count, result.failed_count, result.protected_count), ("applied", 1, 3, 2))
        self.assertEqual(self.state(), (140, {1: (6, 1), 2: (1, 1), 20006: (0, 0)}))

    def test_duplicate_reuses_stored_batch_and_parameter_conflict_is_rejected(self) -> None:
        first = self.apply("repeat")
        # mutable outcomes content must not break same-op replay when attempt count matches
        duplicate = self.apply("repeat", outcomes=(False, False, True, True))
        self.assertEqual((first.status, duplicate.status), ("applied", "duplicate"))
        self.assertEqual((duplicate.successful_count, duplicate.failed_count, duplicate.protected_count), (1, 3, 2))
        self.assertIsNotNone(self.service.get_batch_result("repeat"))
        # different attempt count is different request identity
        conflict = self.apply("repeat", outcomes=(True, False))
        self.assertEqual(conflict.status, "state_changed")
        self.assertEqual(self.state(), (140, {1: (6, 1), 2: (1, 1), 20006: (0, 0)}))

    def test_live_material_shortage_rolls_back_entire_batch(self) -> None:
        result = self.apply("short", outcomes=(True, True, True, True, True))
        self.assertEqual(result.status, "item_insufficient")
        self.assertEqual(self.state(), (200, {1: (10, 1), 20006: (2, 0)}))

    def test_operation_record_failure_rolls_back_every_change(self) -> None:
        with db_backend.transaction(self.database) as conn:
            conn.execute("CREATE TABLE fusion_batch_operations (operation_id TEXT PRIMARY KEY, payload TEXT NOT NULL, outcomes TEXT NOT NULL, successful_count INTEGER NOT NULL, failed_count INTEGER NOT NULL, protected_count INTEGER NOT NULL)")
            conn.execute("CREATE TRIGGER fail_fusion_batch BEFORE INSERT ON fusion_batch_operations BEGIN SELECT RAISE(ABORT, 'failed'); END")
        with self.assertRaises(db_backend.IntegrityError):
            self.apply("rollback")
        self.assertEqual(self.state(), (200, {1: (10, 1), 20006: (2, 0)}))


if __name__ == "__main__":
    unittest.main()
