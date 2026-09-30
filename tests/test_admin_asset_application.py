from __future__ import annotations

import tempfile
import unittest
from pathlib import Path

from nonebot_plugin_xiuxian_2.features.admin_asset.application import AdminAssetApplication
from nonebot_plugin_xiuxian_2.features.admin_asset.migrations import apply_admin_stone_adjustment, apply_admin_stone_batch
from nonebot_plugin_xiuxian_2.features.admin_asset.stone_repository import AdminStoneSqlRepository
from nonebot_plugin_xiuxian_2.infrastructure.database import DatabaseUnitOfWork
from nonebot_plugin_xiuxian_2.plugin import apply_platform_schema


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
    @staticmethod
    def _application(directory: str, *, repository=None, item_repository=None) -> AdminAssetApplication:
        database = Path(directory) / "game.db"
        with DatabaseUnitOfWork(database) as uow:
            apply_platform_schema(uow)
        return AdminAssetApplication(database, repository=repository, item_repository=item_repository)

    def _call(self, app, operation_id="admin-1", *, expected_stone=100, requested_delta=25):
        return app.adjust_stone(
            operation_id=operation_id,
            operator_id="operator-1",
            user_id="user-1",
            expected_stone=expected_stone,
            requested_delta=requested_delta,
            target_name="道友",
        )

    def test_adjustment_is_audited_and_idempotent(self):
        with tempfile.TemporaryDirectory() as directory:
            repository = _Repository()
            app = self._application(directory, repository=repository)
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

    def test_default_stone_repository_and_started_ledger_recovery(self):
        with tempfile.TemporaryDirectory() as directory:
            app = self._application(directory)
            database = Path(directory) / "game.db"
            with DatabaseUnitOfWork(database) as uow:
                apply_admin_stone_adjustment(uow)
                uow.execute("CREATE TABLE user_xiuxian(user_id TEXT PRIMARY KEY,stone INTEGER)")
                uow.execute("INSERT INTO user_xiuxian(user_id,stone) VALUES(?,?)", ("user-1", 100))

            first = self._call(app, "admin-default")
            replay = self._call(app, "admin-default")
            self.assertTrue(first.ok)
            self.assertEqual(first.data["status"], "adjusted")
            self.assertTrue(replay.replayed)

            payload = {
                "operator_id": "operator-1",
                "user_id": "user-1",
                "expected_stone": 125,
                "requested_delta": 25,
                "target_name": "道友",
            }
            AdminStoneSqlRepository(database).adjust(
                "admin-started", "operator-1", "user-1", 125, 25, target_name="道友"
            )
            with DatabaseUnitOfWork(database, immediate=True) as uow:
                app.ledger.begin(uow, "admin-started", "admin.stone_adjust", payload)

            recovered = self._call(app, "admin-started", expected_stone=125)
            self.assertTrue(recovered.ok)
            self.assertEqual(recovered.data["status"], "duplicate")
            with DatabaseUnitOfWork(database) as uow:
                stone = uow.query_one("SELECT stone FROM user_xiuxian WHERE user_id=?", ("user-1",))
                audit_count = uow.query_one(
                    "SELECT COUNT(*) AS count FROM economy_log WHERE user_id=?", ("user-1",)
                )
                ledger = uow.query_one(
                    "SELECT status FROM operation_ledger WHERE operation_id=? AND action=?",
                    ("admin-started", "admin.stone_adjust"),
                )
            self.assertEqual(int(stone["stone"]), 150)
            self.assertEqual(int(audit_count["count"]), 2)
            self.assertEqual(ledger["status"], "applied")

    def test_state_rejection_is_stable(self):
        with tempfile.TemporaryDirectory() as directory:
            repository = _Repository("state_changed")
            app = self._application(directory, repository=repository)
            first = self._call(app, "admin-2")
            second = self._call(app, "admin-2")
            self.assertFalse(first.ok)
            self.assertEqual(first.code, "state_changed")
            self.assertEqual(second.code, "state_changed")
            self.assertEqual(repository.calls, 1)

    def test_global_stone_batch_application_resumes_from_operation_receipt(self):
        with tempfile.TemporaryDirectory() as directory:
            app = self._application(directory)
            database = Path(directory) / "game.db"
            with DatabaseUnitOfWork(database) as uow:
                uow.execute("CREATE TABLE user_xiuxian(user_id TEXT PRIMARY KEY,stone INTEGER)")
                uow.executemany(
                    "INSERT INTO user_xiuxian(user_id,stone) VALUES(?,?)",
                    (("a", 10), ("b", 20)),
                )
                apply_admin_stone_batch(uow)

            first = app.adjust_stone_batch(
                operation_id="global-stone", operator_id="operator-1", requested_delta=5, chunk_size=1
            )
            self.assertEqual(first.completed, 1)
            self.assertEqual(
                app.find_running_stone_batch(operator_id="operator-1", requested_delta=5),
                "global-stone",
            )
            resumed = app.adjust_stone_batch(
                operation_id="global-stone", operator_id="operator-1", requested_delta=5, chunk_size=1
            )
            duplicate = app.adjust_stone_batch(
                operation_id="global-stone", operator_id="operator-1", requested_delta=5, chunk_size=1
            )
            self.assertEqual((resumed.completed, resumed.applied_delta), (2, 10))
            self.assertEqual(duplicate.status, "duplicate")
            with DatabaseUnitOfWork(database) as uow:
                balances = [
                    row["stone"]
                    for row in uow.query_all("SELECT stone FROM user_xiuxian ORDER BY user_id")
                ]
            self.assertEqual(balances, [15, 25])

    def test_zero_delta_is_rejected_before_repository(self):
        with tempfile.TemporaryDirectory() as directory:
            repository = _Repository()
            app = self._application(directory, repository=repository)
            with self.assertRaises(Exception):
                app.adjust_stone(operation_id="admin-3", operator_id="operator-1", user_id="user-1", expected_stone=100, requested_delta=0)
            self.assertEqual(repository.calls, 0)

    def test_item_grant_is_audited_and_idempotent(self):
        with tempfile.TemporaryDirectory() as directory:
            repository = _ItemRepository()
            app = self._application(directory, item_repository=repository)
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
