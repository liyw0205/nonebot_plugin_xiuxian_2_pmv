from __future__ import annotations

import tempfile
import unittest
from pathlib import Path
from unittest.mock import Mock

from ....core.errors import ConflictError
from ..application import WorkClaimApplication, WorkSettlementApplication
from ....infrastructure.database import DatabaseUnitOfWork, OperationLedger


class _Repository:
    def __init__(self, status="applied"):
        self.status = status
        self.calls = 0

    def claim(self, operation_id, user_id, expected_count, expected_offer, task_index, started_at):
        self.calls += 1
        return {"status": self.status, "task_name": "采药", "started_at": started_at, "remaining_count": expected_count}

    def settle(self, *args, **kwargs):
        self.calls += 1
        return {
            "status": self.status,
            "exp": 120,
            "item_awarded": True,
            "success_kind": "ok",
            "item_msg": "一品:灵草",
            "scheduled_time": "采药",
        }


class WorkClaimApplicationTests(unittest.TestCase):
    @staticmethod
    def _application(application_type, database, repository, **kwargs):
        with DatabaseUnitOfWork(database) as uow:
            OperationLedger().ensure_schema(uow)
        return application_type(database, repository=repository, **kwargs)

    def test_claim_replays_without_repeating_repository(self):
        with tempfile.TemporaryDirectory() as directory:
            repository = _Repository()
            app = self._application(
                WorkClaimApplication, Path(directory) / "game.db", repository
            )
            kwargs = {
                "operation_id": "work-1", "user_id": "u", "expected_count": 3,
                "expected_offer": {"tasks": {"采药": {"time": 5}}}, "task_index": 1,
                "started_at": "2026-09-12 10:00:00",
            }
            first = app.claim(**kwargs)
            second = app.claim(**kwargs)
            self.assertTrue(first.ok)
            self.assertTrue(second.replayed)
            self.assertEqual(repository.calls, 1)

    def test_claim_projects_legacy_offer_after_sql_application(self):
        with tempfile.TemporaryDirectory() as directory:
            database = Path(directory) / "game.db"
            repository = _Repository()
            writer = Mock()
            app = self._application(
                WorkClaimApplication,
                database,
                repository,
                legacy_projection_writer=writer,
            )
            offer = {
                "tasks": {"采药": {"time": 5}},
                "task_order": ["采药"],
                "status": 1,
                "refresh_time": "2026-09-12 10:00:00",
            }

            outcome = app.claim(
                operation_id="claim-projection",
                user_id="u",
                expected_count=3,
                expected_offer=offer,
                task_index=1,
                started_at="2026-09-12 10:00:00",
            )

            self.assertTrue(outcome.ok)
            writer.assert_called_once_with(
                "u",
                {
                    "tasks": offer["tasks"],
                    "task_order": offer["task_order"],
                    "status": 2,
                    "refresh_time": offer["refresh_time"],
                    "user_level": None,
                },
            )

    def test_claim_in_flight_receipt_conflicts_without_repository_retry(self):
        with tempfile.TemporaryDirectory() as directory:
            database = Path(directory) / "game.db"
            repository = _Repository()
            ledger = OperationLedger()
            with DatabaseUnitOfWork(database) as uow:
                ledger.ensure_schema(uow)
            kwargs = {
                "operation_id": "claim-in-flight",
                "user_id": "u",
                "expected_count": 3,
                "expected_offer": {"tasks": {"采药": {"time": 5}}},
                "task_index": 1,
                "started_at": "2026-09-12 10:00:00",
            }
            with DatabaseUnitOfWork(database, immediate=True) as uow:
                self.assertIsNone(
                    ledger.begin(
                        uow,
                        kwargs["operation_id"],
                        "work.claim",
                        {
                            "user_id": kwargs["user_id"],
                            "expected_count": kwargs["expected_count"],
                            "task_index": kwargs["task_index"],
                            "started_at": kwargs["started_at"],
                        },
                    )
                )
            app = WorkClaimApplication(database, repository=repository, ledger=ledger)

            with self.assertRaises(ConflictError):
                app.claim(**kwargs)

            self.assertEqual(repository.calls, 0)

    def test_settlement_replays_without_repeating_repository(self):
        with tempfile.TemporaryDirectory() as directory:
            repository = _Repository()
            app = self._application(
                WorkSettlementApplication, Path(directory) / "game.db", repository
            )
            kwargs = {
                "operation_id": "work-settle-1", "user_id": "u",
                "expected_work": {"create_time": "2026-09-12 10:00:00", "scheduled_time": "采药"},
                "exp_gain": 120, "item": {"id": 1, "name": "灵草", "type": "药材"},
                "max_exp": 999, "max_goods_num": 99, "success_kind": "ok", "item_msg": "一品:灵草",
            }
            first = app.settle(**kwargs)
            second = app.settle(**kwargs)
            self.assertTrue(first.ok)
            self.assertTrue(second.replayed)
            self.assertEqual(first.data["exp"], 120)
            self.assertEqual(repository.calls, 1)

    def test_settlement_clears_legacy_projection_only_after_new_success(self):
        with tempfile.TemporaryDirectory() as directory:
            database = Path(directory) / "game.db"
            deleter = Mock()
            kwargs = {
                "operation_id": "work-settle-cleanup",
                "user_id": "u",
                "expected_work": {"create_time": "start", "scheduled_time": "采药"},
                "exp_gain": 10,
                "item": None,
                "max_exp": 100,
                "max_goods_num": 99,
            }
            applied = self._application(
                WorkSettlementApplication,
                database,
                _Repository("applied"),
                legacy_projection_deleter=deleter,
            )
            rejected = self._application(
                WorkSettlementApplication,
                Path(directory) / "rejected.db",
                _Repository("state_changed"),
                legacy_projection_deleter=deleter,
            )

            self.assertTrue(applied.settle(**kwargs).ok)
            deleter.assert_called_once_with("u")
            rejected.settle(**{**kwargs, "operation_id": "work-settle-stale"})
            deleter.assert_called_once_with("u")

    def test_state_rejection_is_idempotent(self):
        with tempfile.TemporaryDirectory() as directory:
            repository = _Repository("state_changed")
            app = self._application(
                WorkClaimApplication, Path(directory) / "game.db", repository
            )
            kwargs = {
                "operation_id": "work-2", "user_id": "u", "expected_count": 0,
                "expected_offer": {"tasks": {}}, "task_index": 1, "started_at": "2026-09-12 10:00:00",
            }
            first = app.claim(**kwargs)
            second = app.claim(**kwargs)
            self.assertFalse(first.ok)
            self.assertEqual(second.code, "state_changed")
            self.assertEqual(repository.calls, 1)

    def test_claim_schema_missing_is_explicit_without_request_ddl(self):
        with tempfile.TemporaryDirectory() as directory:
            database = Path(directory) / "game.db"
            with DatabaseUnitOfWork(database) as uow:
                uow.execute("CREATE TABLE user_xiuxian(user_id TEXT PRIMARY KEY,work_num INTEGER)")
                uow.execute("CREATE TABLE user_cd(user_id TEXT PRIMARY KEY,type INTEGER,create_time TEXT,scheduled_time TEXT)")
                uow.execute("INSERT INTO user_xiuxian VALUES('u',3)")
                uow.execute("INSERT INTO user_cd VALUES('u',0,'0',NULL)")
                OperationLedger().ensure_schema(uow)

            outcome = WorkClaimApplication(database).claim(
                operation_id="missing-schema",
                user_id="u",
                expected_count=3,
                expected_offer={"tasks": {"采药": {"time": 5}}},
                task_index=1,
                started_at="2026-09-28 10:00:00",
            )

            self.assertEqual(outcome.code, "schema_missing")
            self.assertIn("尚未完成升级", outcome.message)
            with DatabaseUnitOfWork(database, read_only=True) as uow:
                tables = {
                    str(row["name"])
                    for row in uow.query_all("SELECT name FROM sqlite_master WHERE type='table'")
                }
            self.assertNotIn("work_claim_operations", tables)
            self.assertNotIn("work_active_snapshots", tables)


if __name__ == "__main__":
    unittest.main()
