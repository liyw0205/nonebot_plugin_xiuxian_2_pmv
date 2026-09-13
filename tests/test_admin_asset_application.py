from __future__ import annotations

import tempfile
import unittest
from pathlib import Path

from nonebot_plugin_xiuxian_2.features.admin_asset.application import AdminAssetApplication
from nonebot_plugin_xiuxian_2.infrastructure.database import DatabaseUnitOfWork


class _Repository:
    def __init__(self, status: str = "adjusted") -> None:
        self.status = status
        self.calls = 0

    def adjust(self, operation_id, operator_id, user_id, expected_stone, requested_delta, *, target_name=""):
        self.calls += 1
        return {
            "status": self.status,
            "previous_stone": expected_stone,
            "final_stone": max(0, expected_stone + requested_delta),
            "applied_delta": requested_delta,
        }


class _ItemRepository:
    def __init__(self, status: str = "granted") -> None:
        self.status = status
        self.calls = 0

    def grant(self, operation_id, operator_id, user_id, item_id, item_name, item_type, quantity, expected_quantity, max_goods_num, *, target_name=""):
        self.calls += 1
        return {
            "status": self.status,
            "previous_quantity": expected_quantity,
            "final_quantity": expected_quantity + quantity,
            "granted_quantity": quantity,
        }


class AdminAssetApplicationTests(unittest.TestCase):
    def _call(self, app, operation_id="admin-1"):
        return app.adjust_stone(
            operation_id=operation_id,
            operator_id="operator-1",
            user_id="user-1",
            expected_stone=100,
            requested_delta=25,
            target_name="道友",
        )

    def test_adjustment_is_audited_and_idempotent(self):
        with tempfile.TemporaryDirectory() as directory:
            repository = _Repository()
            app = AdminAssetApplication(Path(directory) / "game.db", repository=repository)
            first = self._call(app)
            second = self._call(app)
            self.assertTrue(first.ok)
            self.assertTrue(second.replayed)
            self.assertEqual(repository.calls, 1)
            with DatabaseUnitOfWork(Path(directory) / "game.db") as uow:
                ledger = uow.query_one("SELECT status FROM operation_ledger WHERE operation_id=?", ("admin-1",))
                audit = uow.query_one("SELECT category FROM operation_audit WHERE operation_id=?", ("admin-1",))
            self.assertEqual(ledger["status"], "applied")
            self.assertEqual(audit["category"], "admin_asset")

    def test_state_rejection_is_stable(self):
        with tempfile.TemporaryDirectory() as directory:
            repository = _Repository("state_changed")
            app = AdminAssetApplication(Path(directory) / "game.db", repository=repository)
            first = self._call(app, "admin-2")
            second = self._call(app, "admin-2")
            self.assertFalse(first.ok)
            self.assertEqual(first.code, "state_changed")
            self.assertEqual(second.code, "state_changed")
            self.assertEqual(repository.calls, 1)

    def test_zero_delta_is_rejected_before_repository(self):
        with tempfile.TemporaryDirectory() as directory:
            repository = _Repository()
            app = AdminAssetApplication(Path(directory) / "game.db", repository=repository)
            with self.assertRaises(Exception):
                app.adjust_stone(operation_id="admin-3", operator_id="operator-1", user_id="user-1", expected_stone=100, requested_delta=0)
            self.assertEqual(repository.calls, 0)

    def test_item_grant_is_audited_and_idempotent(self):
        with tempfile.TemporaryDirectory() as directory:
            repository = _ItemRepository()
            app = AdminAssetApplication(Path(directory) / "game.db", item_repository=repository)
            kwargs = {
                "operation_id": "item-1", "operator_id": "operator-1", "user_id": "user-1",
                "item_id": 9, "item_name": "丹药", "item_type": "丹药", "quantity": 2,
                "expected_quantity": 3, "max_goods_num": 99,
            }
            first = app.grant_item(**kwargs)
            second = app.grant_item(**kwargs)
            self.assertTrue(first.ok)
            self.assertTrue(second.replayed)
            self.assertEqual(repository.calls, 1)
            with DatabaseUnitOfWork(Path(directory) / "game.db") as uow:
                audit = uow.query_one("SELECT category FROM operation_audit WHERE operation_id=? AND action=?", ("item-1", "admin.item_grant"))
            self.assertEqual(audit["category"], "admin_asset")


if __name__ == "__main__":
    unittest.main()
