from __future__ import annotations

import sqlite3
import tempfile
import unittest
from pathlib import Path

from nonebot_plugin_xiuxian_2.features.bank.migrations import apply_bank_accounts
from nonebot_plugin_xiuxian_2.infrastructure.database import DatabaseUnitOfWork


class BankAccountsMigrationTests(unittest.TestCase):
    def test_new_bank_tables_are_idempotent(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            database = Path(directory) / "game.db"
            with DatabaseUnitOfWork(database) as uow:
                apply_bank_accounts(uow)
            with DatabaseUnitOfWork(database) as uow:
                apply_bank_accounts(uow)
            connection = sqlite3.connect(database)
            names = {row[0] for row in connection.execute("select name from sqlite_master where type='table'")}
            self.assertTrue({"bank_accounts", "bank_account_operations"}.issubset(names))


if __name__ == "__main__":
    unittest.main()
