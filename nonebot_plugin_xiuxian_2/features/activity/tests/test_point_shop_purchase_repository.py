from __future__ import annotations

import tempfile
import unittest
from pathlib import Path

from ..point_shop_purchase_repository import ActivityPointShopPurchaseSqlRepository
from ..migrations import apply_activity_event_receipts, apply_activity_state_schema
from ....infrastructure.database import DatabaseUnitOfWork, OperationLedger


class ActivityPointShopPurchaseRepositoryTests(unittest.TestCase):
    rewards = (
        {"type": "stone", "quantity": 50},
        {"id": 101, "name": "活动令", "type": "道具", "quantity": 2},
    )

    def setUp(self) -> None:
        self.temp = tempfile.TemporaryDirectory()
        self.database = Path(self.temp.name) / "game.db"
        with DatabaseUnitOfWork(self.database) as uow:
            apply_activity_state_schema(uow)
            apply_activity_event_receipts(uow)
            OperationLedger().ensure_schema(uow)
            uow.execute("CREATE TABLE user_xiuxian(user_id TEXT PRIMARY KEY,stone INTEGER)")
            uow.execute("INSERT INTO user_xiuxian VALUES('u',10)")
            uow.execute(
                "CREATE TABLE back(user_id TEXT,goods_id INTEGER,goods_name TEXT,"
                "goods_type TEXT,goods_num INTEGER,create_time TEXT,update_time TEXT,"
                "bind_num INTEGER,UNIQUE(user_id,goods_id))"
            )
            uow.execute(
                "INSERT INTO activity_point_balance(activity_key,user_id,points,total_points) "
                "VALUES('a','u',1000,1200)"
            )
        self.repository = ActivityPointShopPurchaseSqlRepository(self.database)

    def tearDown(self) -> None:
        self.temp.cleanup()

    def purchase(self, operation_id: str = "op-1", **overrides):
        arguments = {
            "operation_id": operation_id,
            "user_id": "u",
            "activity_key": "a",
            "item_key": "i",
            "quantity": 2,
            "unit_cost": 100,
            "personal_limit": 3,
            "stock_limit": 4,
            "rewards": self.rewards,
            "max_goods_num": 100,
        }
        arguments.update(overrides)
        return self.repository.purchase(**arguments)

    def test_purchase_and_receipt_are_atomic_and_idempotent(self) -> None:
        first = self.purchase()
        replay = self.purchase()

        self.assertEqual("applied", first.status)
        self.assertEqual("duplicate", replay.status)
        self.assertEqual((2, 200, 800, 2, 2), (
            replay.quantity,
            replay.cost,
            replay.points,
            replay.personal_count,
            replay.total_count,
        ))
        with DatabaseUnitOfWork(self.database, read_only=True) as uow:
            self.assertEqual(800, uow.query_one(
                "SELECT points FROM activity_point_balance WHERE user_id='u'"
            )["points"])
            self.assertEqual(2, uow.query_one(
                "SELECT count FROM activity_point_purchase WHERE user_id='u'"
            )["count"])
            self.assertEqual(60, uow.query_one(
                "SELECT stone FROM user_xiuxian WHERE user_id='u'"
            )["stone"])
            self.assertEqual(2, uow.query_one(
                "SELECT goods_num FROM back WHERE user_id='u'"
            )["goods_num"])

    def test_conflicts_limits_and_inventory_rejections_do_not_mutate(self) -> None:
        self.assertEqual("applied", self.purchase().status)
        self.assertEqual("operation_conflict", self.purchase(unit_cost=101).status)
        self.assertEqual("personal_limit", self.purchase("too-many", quantity=2).status)
        self.assertEqual("stock_insufficient", self.purchase(
            "stock", quantity=3, personal_limit=0
        ).status)
        self.assertEqual("inventory_full", self.purchase(
            "full", quantity=1, unit_cost=1, personal_limit=0,
            rewards=({"id": 102, "name": "满包物品", "type": "道具", "quantity": 101},),
        ).status)
        with DatabaseUnitOfWork(self.database, read_only=True) as uow:
            self.assertEqual(800, uow.query_one(
                "SELECT points FROM activity_point_balance WHERE user_id='u'"
            )["points"])
            self.assertEqual(60, uow.query_one(
                "SELECT stone FROM user_xiuxian WHERE user_id='u'"
            )["stone"])

    def test_receipt_write_failure_rolls_back_points_and_rewards(self) -> None:
        with DatabaseUnitOfWork(self.database) as uow:
            uow.execute(
                "CREATE TRIGGER fail_purchase_receipt BEFORE INSERT "
                "ON activity_point_purchase_operations "
                "BEGIN SELECT RAISE(ABORT,'forced failure'); END"
            )

        with self.assertRaisesRegex(Exception, "forced failure"):
            self.purchase("rollback")

        with DatabaseUnitOfWork(self.database, read_only=True) as uow:
            self.assertEqual(1000, uow.query_one(
                "SELECT points FROM activity_point_balance WHERE user_id='u'"
            )["points"])
            self.assertIsNone(uow.query_one("SELECT 1 FROM activity_point_purchase"))
            self.assertEqual(10, uow.query_one(
                "SELECT stone FROM user_xiuxian WHERE user_id='u'"
            )["stone"])
            self.assertIsNone(uow.query_one("SELECT 1 FROM back"))


if __name__ == "__main__":
    unittest.main()
