from __future__ import annotations

import tempfile
import unittest
from pathlib import Path

import nonebot

nonebot.init()

from nonebot_plugin_xiuxian_2.xiuxian.xiuxian_mixelixir.transaction_service import (
    MixelixirSettlementService,
)
from tests.test_db_backend import db_backend


class MixelixirSettlementServiceTests(unittest.TestCase):
    def setUp(self) -> None:
        self.temp_dir = tempfile.TemporaryDirectory()
        self.database = Path(self.temp_dir.name) / "mixelixir.sqlite3"
        with db_backend.transaction(self.database) as conn:
            conn.execute(
                "CREATE TABLE user_xiuxian (user_id TEXT PRIMARY KEY, mixelixir_num INTEGER NOT NULL)"
            )
            conn.execute(
                "CREATE TABLE back (user_id TEXT NOT NULL, goods_id INTEGER NOT NULL, goods_name TEXT, "
                "goods_type TEXT, goods_num INTEGER NOT NULL, bind_num INTEGER DEFAULT 0, "
                "UNIQUE(user_id, goods_id))"
            )
            conn.execute("INSERT INTO user_xiuxian VALUES (%s, %s)", ("user", 4))
            conn.execute("INSERT INTO back VALUES (%s, %s, %s, %s, %s, %s)", ("user", 1, "主药", "药材", 5, 0))
            conn.execute("INSERT INTO back VALUES (%s, %s, %s, %s, %s, %s)", ("user", 2, "药引", "药材", 4, 0))
        self.service = MixelixirSettlementService(self.database)

    def tearDown(self) -> None:
        self.temp_dir.cleanup()

    def state(self):
        with db_backend.connection(self.database) as conn:
            count = int(conn.execute("SELECT mixelixir_num FROM user_xiuxian WHERE user_id=%s", ("user",)).fetchone()[0])
            items = {int(row[0]): int(row[1]) for row in conn.execute("SELECT goods_id, goods_num FROM back WHERE user_id=%s", ("user",)).fetchall()}
            return count, items

    def settle(self, operation_id="settle", materials=None, quantity=3):
        return self.service.settle(
            operation_id, "user", materials or {1: 2, 2: 1}, 100, "成品丹", quantity,
            max_goods_num=999,
        )

    def test_success_consumes_all_materials_and_grants_reward(self) -> None:
        result = self.settle()
        self.assertEqual((result.status, result.reward_quantity), ("applied", 3))
        self.assertEqual(self.state(), (5, {1: 3, 2: 3, 100: 3}))

    def test_same_material_quantities_are_consumed_as_one_total(self) -> None:
        result = self.settle(materials={1: 4})
        self.assertEqual(result.status, "applied")
        self.assertEqual(self.state(), (5, {1: 1, 2: 4, 100: 3}))

    def test_insufficient_material_changes_nothing(self) -> None:
        result = self.settle(materials={1: 6})
        self.assertEqual(result.status, "item_insufficient")
        self.assertEqual(self.state(), (4, {1: 5, 2: 4}))

    def test_duplicate_reuses_result_and_conflict_is_rejected(self) -> None:
        first = self.settle("repeat")
        duplicate = self.settle("repeat")
        conflict = self.settle("repeat", quantity=4)
        self.assertEqual((first.status, duplicate.status, conflict.status), ("applied", "duplicate", "state_changed"))
        self.assertEqual(duplicate.reward_quantity, 3)
        self.assertEqual(self.state(), (5, {1: 3, 2: 3, 100: 3}))

    def test_operation_failure_rolls_back_every_change(self) -> None:
        with db_backend.transaction(self.database) as conn:
            conn.execute(
                "CREATE TABLE mixelixir_settlement_operations (operation_id TEXT PRIMARY KEY, payload TEXT NOT NULL, reward_quantity INTEGER NOT NULL)"
            )
            conn.execute(
                "CREATE TRIGGER fail_mixelixir BEFORE INSERT ON mixelixir_settlement_operations "
                "BEGIN SELECT RAISE(ABORT, 'failed'); END"
            )
        with self.assertRaises(db_backend.IntegrityError):
            self.settle("rollback")
        self.assertEqual(self.state(), (4, {1: 5, 2: 4}))


if __name__ == "__main__":
    unittest.main()
