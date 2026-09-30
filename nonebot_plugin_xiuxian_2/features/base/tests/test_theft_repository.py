import sqlite3
import tempfile
import unittest
from pathlib import Path

from ....infrastructure.database import DatabaseUnitOfWork
from ..migrations import apply_base_stone_contest_operations
from ..theft_repository import BaseStoneTheftSqlRepository


class BaseStoneTheftRepositoryTests(unittest.TestCase):
    def setUp(self) -> None:
        self.temp_dir = tempfile.TemporaryDirectory()
        self.database = Path(self.temp_dir.name) / "game.sqlite3"
        with DatabaseUnitOfWork(self.database, immediate=True) as uow:
            uow.execute(
                "CREATE TABLE user_xiuxian("
                "user_id TEXT PRIMARY KEY,stone INTEGER NOT NULL,user_stamina INTEGER NOT NULL)"
            )
            uow.execute("INSERT INTO user_xiuxian VALUES('thief',100,20)")
            uow.execute("INSERT INTO user_xiuxian VALUES('victim',30,20)")
            apply_base_stone_contest_operations(uow)
        self.repository = BaseStoneTheftSqlRepository(self.database)

    def tearDown(self) -> None:
        self.temp_dir.cleanup()

    def _state(self):
        with DatabaseUnitOfWork(self.database, read_only=True) as uow:
            return {
                str(row["user_id"]): (int(row["stone"]), int(row["user_stamina"]))
                for row in uow.query_all(
                    "SELECT user_id,stone,user_stamina FROM user_xiuxian ORDER BY user_id"
                )
            }

    def test_successful_theft_is_atomic_and_replayable(self):
        result = self.repository.settle(
            "steal-1", "thief", "victim", outcome="success",
            requested_amount=20, penalty_amount=10,
        )
        duplicate = self.repository.get_result("steal-1", "thief", "victim")

        self.assertEqual((result.status, result.transferred_amount, result.payer_balance), ("settled", 20, 10))
        self.assertEqual((duplicate.status, duplicate.outcome), ("duplicate", "success"))
        self.assertEqual(self._state(), {"thief": (120, 10), "victim": (10, 20)})

    def test_failure_charges_penalty_and_stamina(self):
        result = self.repository.settle(
            "steal-failed", "thief", "victim", outcome="failure",
            requested_amount=10, penalty_amount=10,
        )

        self.assertEqual((result.status, result.transferred_amount), ("settled", 10))
        self.assertEqual(self._state(), {"thief": (90, 10), "victim": (40, 20)})

    def test_replay_conflict_and_insufficient_state_do_not_mutate_assets(self):
        self.repository.settle(
            "steal-2", "thief", "victim", outcome="success",
            requested_amount=20, penalty_amount=10,
        )
        conflict = self.repository.settle(
            "steal-2", "thief", "other", outcome="failure",
            requested_amount=10, penalty_amount=10,
        )
        with DatabaseUnitOfWork(self.database, immediate=True) as uow:
            uow.execute("UPDATE user_xiuxian SET user_stamina=1 WHERE user_id='thief'")
        insufficient = self.repository.settle(
            "steal-tired", "thief", "victim", outcome="success",
            requested_amount=10, penalty_amount=10,
        )

        self.assertEqual(conflict.status, "operation_conflict")
        self.assertEqual(insufficient.status, "stamina_insufficient")
        self.assertEqual(self._state(), {"thief": (120, 1), "victim": (10, 20)})

    def test_missing_startup_schema_fails_closed_without_ddl_or_database_creation(self):
        with tempfile.TemporaryDirectory() as temp:
            database = Path(temp) / "not-created.sqlite3"
            result = BaseStoneTheftSqlRepository(database).settle(
                "no-schema", "thief", "victim", outcome="success",
                requested_amount=1, penalty_amount=1,
            )
            self.assertEqual(result.status, "schema_missing")
            self.assertFalse(database.exists())

        with tempfile.TemporaryDirectory() as temp:
            database = Path(temp) / "legacy.sqlite3"
            with DatabaseUnitOfWork(database, immediate=True) as uow:
                uow.execute("CREATE TABLE user_xiuxian(user_id TEXT,stone INTEGER,user_stamina INTEGER)")
            result = BaseStoneTheftSqlRepository(database).settle(
                "no-receipt-schema", "thief", "victim", outcome="success",
                requested_amount=1, penalty_amount=1,
            )
            self.assertEqual(result.status, "schema_missing")
            with DatabaseUnitOfWork(database, read_only=True) as uow:
                tables = {
                    str(row["name"])
                    for row in uow.query_all("SELECT name FROM sqlite_master WHERE type='table'")
                }
            self.assertNotIn("stone_contest_operations", tables)

    def test_migration_adds_legacy_columns_without_changing_receipts(self):
        with tempfile.TemporaryDirectory() as temp:
            database = Path(temp) / "legacy.sqlite3"
            with DatabaseUnitOfWork(database, immediate=True) as uow:
                uow.execute(
                    "CREATE TABLE stone_contest_operations("
                    "operation_id TEXT PRIMARY KEY,payer_id TEXT NOT NULL,receiver_id TEXT NOT NULL,"
                    "requested_amount INTEGER NOT NULL,transferred_amount INTEGER NOT NULL,"
                    "payer_balance INTEGER NOT NULL)"
                )
                uow.execute(
                    "INSERT INTO stone_contest_operations VALUES('old-transfer','thief','victim',5,5,95)"
                )
                apply_base_stone_contest_operations(uow)
                apply_base_stone_contest_operations(uow)
                columns = {
                    str(row["name"])
                    for row in uow.query_all('PRAGMA table_info("stone_contest_operations")')
                }
                receipt = uow.query_one(
                    "SELECT payer_id,receiver_id,transferred_amount,operation_type "
                    "FROM stone_contest_operations WHERE operation_id='old-transfer'"
                )

            self.assertIn("thief_id", columns)
            self.assertEqual(
                (receipt["payer_id"], receipt["receiver_id"], receipt["transferred_amount"], receipt["operation_type"]),
                ("thief", "victim", 5, "transfer"),
            )

    def test_receipt_failure_rolls_back_balances_and_stamina(self):
        with DatabaseUnitOfWork(self.database, immediate=True) as uow:
            uow.execute(
                "CREATE TRIGGER fail_theft_receipt BEFORE INSERT ON stone_contest_operations "
                "WHEN NEW.operation_type='theft' BEGIN SELECT RAISE(ABORT,'receipt failed'); END"
            )

        with self.assertRaises(sqlite3.IntegrityError):
            self.repository.settle(
                "steal-write-fail", "thief", "victim", outcome="success",
                requested_amount=20, penalty_amount=10,
            )

        self.assertEqual(self._state(), {"thief": (100, 20), "victim": (30, 20)})

    def test_partial_compare_and_set_failure_rolls_back_all_asset_updates(self):
        with DatabaseUnitOfWork(self.database, immediate=True) as uow:
            uow.execute(
                "CREATE TRIGGER ignore_thief_credit BEFORE UPDATE ON user_xiuxian "
                "WHEN OLD.user_id='thief' AND NEW.stone>OLD.stone "
                "BEGIN SELECT RAISE(IGNORE); END"
            )

        result = self.repository.settle(
            "steal-cas-fail", "thief", "victim", outcome="success",
            requested_amount=20, penalty_amount=10,
        )

        self.assertEqual(result.status, "state_changed")
        self.assertEqual(self._state(), {"thief": (100, 20), "victim": (30, 20)})


if __name__ == "__main__":
    unittest.main()
