from __future__ import annotations

import tempfile
import unittest
from pathlib import Path

import nonebot

nonebot.init()

from nonebot_plugin_xiuxian_2.xiuxian.xiuxian_base.pill_fusion_service import (
    PillFusionService,
)
from tests.test_db_backend import db_backend


class PillFusionServiceTests(unittest.TestCase):
    def setUp(self) -> None:
        self.temp_dir = tempfile.TemporaryDirectory()
        self.database = Path(self.temp_dir.name) / "pill-fusion.sqlite3"
        with db_backend.transaction(self.database) as conn:
            conn.execute(
                "CREATE TABLE back (user_id TEXT, goods_id INTEGER, goods_name TEXT, "
                "goods_type TEXT, goods_num INTEGER, bind_num INTEGER, "
                "UNIQUE(user_id, goods_id))"
            )
            conn.execute(
                "INSERT INTO back VALUES (%s, %s, %s, %s, %s, %s)",
                ("user", 1999, "渡厄丹", "丹药", 8, 5),
            )
        self.service = PillFusionService(self.database)

    def tearDown(self) -> None:
        self.temp_dir.cleanup()

    def inventory(self, goods_id: int):
        with db_backend.connection(self.database) as conn:
            row = conn.execute(
                "SELECT goods_num, bind_num FROM back WHERE user_id=%s AND goods_id=%s",
                ("user", goods_id),
            ).fetchone()
        return None if row is None else tuple(map(int, row))

    def operation_count(self) -> int:
        with db_backend.connection(self.database) as conn:
            exists = conn.execute(
                "SELECT 1 FROM sqlite_master WHERE type='table' AND name=%s",
                ("pill_fusion_operations",),
            ).fetchone()
            if not exists:
                return 0
            return int(conn.execute("SELECT COUNT(*) FROM pill_fusion_operations").fetchone()[0])

    def fuse(self, operation_id: str, quantity: int, successful: bool):
        return self.service.apply(
            operation_id,
            "user",
            1999,
            quantity,
            1996,
            "天命丹",
            "丹药",
            successful=successful,
            max_goods_num=1000,
        )

    def test_success_consumes_materials_and_grants_bound_pill_atomically(self) -> None:
        result = self.fuse("fusion-success", 3, True)
        self.assertEqual((result.status, result.successful), ("applied", True))
        self.assertEqual(self.inventory(1999), (5, 2))
        self.assertEqual(self.inventory(1996), (1, 1))
        self.assertEqual(self.operation_count(), 1)

    def test_failed_roll_consumes_materials_without_granting_pill(self) -> None:
        result = self.fuse("fusion-failed", 2, False)
        self.assertEqual((result.status, result.successful), ("applied", False))
        self.assertEqual(self.inventory(1999), (6, 3))
        self.assertIsNone(self.inventory(1996))
        self.assertEqual(self.operation_count(), 1)

    def test_duplicate_reuses_first_roll_without_consuming_or_granting_twice(self) -> None:
        first = self.fuse("fusion-repeat", 2, True)
        second = self.fuse("fusion-repeat", 2, False)
        self.assertEqual((first.status, second.status), ("applied", "duplicate"))
        self.assertTrue(second.successful)
        self.assertEqual(second.target_quantity, 1)
        self.assertEqual(self.inventory(1999), (6, 3))
        self.assertEqual(self.inventory(1996), (1, 1))
        self.assertEqual(self.operation_count(), 1)

    def test_insufficient_materials_do_not_grant_target(self) -> None:
        result = self.fuse("fusion-short", 9, True)
        self.assertEqual(result.status, "item_insufficient")
        self.assertEqual(self.inventory(1999), (8, 5))
        self.assertIsNone(self.inventory(1996))
        self.assertEqual(self.operation_count(), 0)

    def test_operation_failure_rolls_back_materials_and_target(self) -> None:
        with db_backend.transaction(self.database) as conn:
            self.service._ensure_operations(conn)
            conn.execute(
                "CREATE TRIGGER fail_pill_fusion BEFORE INSERT ON pill_fusion_operations "
                "BEGIN SELECT RAISE(ABORT, 'operation failed'); END"
            )
        with self.assertRaises(db_backend.IntegrityError):
            self.fuse("fusion-error", 2, True)
        self.assertEqual(self.inventory(1999), (8, 5))
        self.assertIsNone(self.inventory(1996))
        self.assertEqual(self.operation_count(), 0)

    def test_success_merges_into_existing_target_inventory(self) -> None:
        with db_backend.transaction(self.database) as conn:
            conn.execute(
                "INSERT INTO back VALUES (%s, %s, %s, %s, %s, %s)",
                ("user", 1996, "天命丹", "丹药", 2, 1),
            )

        result = self.fuse("fusion-merge", 2, True)

        self.assertEqual((result.status, result.successful), ("applied", True))
        self.assertEqual(self.inventory(1999), (6, 3))
        self.assertEqual(self.inventory(1996), (3, 2))
        self.assertEqual(self.operation_count(), 1)

    def test_success_target_quantity_is_capped_by_inventory_limit(self) -> None:
        with db_backend.transaction(self.database) as conn:
            conn.execute(
                "INSERT INTO back VALUES (%s, %s, %s, %s, %s, %s)",
                ("user", 1996, "天命丹", "丹药", 4, 4),
            )

        result = self.service.apply(
            "fusion-cap",
            "user",
            1999,
            2,
            1996,
            "天命丹",
            "丹药",
            successful=True,
            target_quantity=3,
            max_goods_num=5,
        )

        self.assertEqual((result.status, result.successful), ("applied", True))
        self.assertEqual(result.target_quantity, 3)
        self.assertEqual(self.inventory(1999), (6, 3))
        self.assertEqual(self.inventory(1996), (5, 5))
        self.assertEqual(self.operation_count(), 1)


if __name__ == "__main__":
    unittest.main()
