from __future__ import annotations

import sqlite3
import tempfile
import unittest
from pathlib import Path

from nonebot_plugin_xiuxian_2.infrastructure.database import DatabaseUnitOfWork
from nonebot_plugin_xiuxian_2.features.base.contest_repository import BaseStoneContestSqlRepository
from nonebot_plugin_xiuxian_2.features.base.migrations import apply_base_stone_contest_operations


class BaseStoneContestRepositoryTests(unittest.TestCase):
    def setUp(self) -> None:
        self.temp_dir = tempfile.TemporaryDirectory()
        self.database = Path(self.temp_dir.name) / "game.sqlite3"
        with DatabaseUnitOfWork(self.database, immediate=True) as uow:
            uow.execute("CREATE TABLE user_xiuxian(user_id TEXT PRIMARY KEY,stone INTEGER NOT NULL)")
            uow.execute("INSERT INTO user_xiuxian VALUES('payer',100)")
            uow.execute("INSERT INTO user_xiuxian VALUES('receiver',10)")
            apply_base_stone_contest_operations(uow)
        self.repository = BaseStoneContestSqlRepository(self.database)

    def tearDown(self) -> None:
        self.temp_dir.cleanup()

    def _balances(self):
        with DatabaseUnitOfWork(self.database, read_only=True) as uow:
            return {
                str(row["user_id"]): int(row["stone"])
                for row in uow.query_all("SELECT user_id,stone FROM user_xiuxian")
            }

    def test_transfer_is_atomic_and_replayable(self):
        first = self.repository.transfer("contest-1", "payer", "receiver", 30)
        second = self.repository.transfer("contest-1", "payer", "receiver", 30)
        self.assertEqual((first.status, first.transferred_amount, first.payer_balance), ("transferred", 30, 70))
        self.assertEqual(second.status, "duplicate")
        self.assertEqual(self._balances(), {"payer": 70, "receiver": 40})

    def test_conflict_and_empty_balance_do_not_mutate(self):
        self.repository.transfer("contest-2", "payer", "receiver", 100)
        conflict = self.repository.transfer("contest-2", "payer", "receiver", 1)
        empty = self.repository.transfer("contest-3", "payer", "receiver", 1)
        self.assertEqual(conflict.status, "state_changed")
        self.assertEqual(empty.status, "payer_empty")
        self.assertEqual(self._balances(), {"payer": 0, "receiver": 110})

    def test_missing_schema_fails_closed_without_creating_database(self):
        with tempfile.TemporaryDirectory() as directory:
            database = Path(directory) / "missing.sqlite3"
            result = BaseStoneContestSqlRepository(database).transfer("missing", "payer", "receiver", 1)
            self.assertEqual(result.status, "schema_missing")
            self.assertFalse(database.exists())

    def test_receipt_failure_rolls_back_asset_updates(self):
        with DatabaseUnitOfWork(self.database, immediate=True) as uow:
            uow.execute(
                "CREATE TRIGGER fail_contest_receipt BEFORE INSERT ON stone_contest_operations "
                "BEGIN SELECT RAISE(ABORT,'receipt failed'); END"
            )
        with self.assertRaises(sqlite3.IntegrityError):
            self.repository.transfer("contest-fail", "payer", "receiver", 30)
        self.assertEqual(self._balances(), {"payer": 100, "receiver": 10})


if __name__ == "__main__":
    unittest.main()
