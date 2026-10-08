from __future__ import annotations

import tempfile
import unittest
from pathlib import Path

from ....infrastructure.database import DatabaseUnitOfWork
from ..migrations import apply_economy_ledger_read_indexes


class EconomyLedgerMigrationTest(unittest.TestCase):
    def test_missing_table_is_ignored(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            with DatabaseUnitOfWork(Path(directory) / "game.db") as uow:
                apply_economy_ledger_read_indexes(uow)
                self.assertEqual([], uow.query_all("SELECT name FROM sqlite_master WHERE type='index'"))

    def test_indexes_follow_available_columns_and_are_idempotent(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            database = Path(directory) / "game.db"
            with DatabaseUnitOfWork(database) as uow:
                uow.execute("CREATE TABLE economy_log(user_id TEXT,source TEXT,created_at TEXT)")
                apply_economy_ledger_read_indexes(uow)
                apply_economy_ledger_read_indexes(uow)
                indexes = {
                    row["name"]
                    for row in uow.query_all(
                        "SELECT name FROM sqlite_master WHERE type='index' AND tbl_name='economy_log'"
                    )
                }
            self.assertEqual(
                {"idx_economy_log_created_at", "idx_economy_log_user_source_time"},
                indexes,
            )


if __name__ == "__main__":
    unittest.main()
