import tempfile
import unittest
from pathlib import Path

from nonebot_plugin_xiuxian_2.features.bank.account_repository import BankAccountRepository
from nonebot_plugin_xiuxian_2.features.bank.migrations import apply_bank_accounts
from nonebot_plugin_xiuxian_2.infrastructure.database import DatabaseUnitOfWork


class BankAccountRepositorySchemaTests(unittest.TestCase):
    def test_operation_requires_startup_schema(self):
        with tempfile.TemporaryDirectory() as directory:
            database = Path(directory) / "game.db"
            repository = BankAccountRepository()
            with DatabaseUnitOfWork(database) as uow:
                apply_bank_accounts(uow)
                self.assertIsNone(repository.operation(uow, "missing"))

    def test_missing_schema_is_not_created_at_request_time(self):
        with tempfile.TemporaryDirectory() as directory:
            database = Path(directory) / "empty.db"
            repository = BankAccountRepository()
            with DatabaseUnitOfWork(database) as uow:
                with self.assertRaises(Exception):
                    repository.operation(uow, "missing")
            with DatabaseUnitOfWork(database) as uow:
                table = uow.query_one("SELECT name FROM sqlite_master WHERE type='table' AND name='bank_account_operations'")
            self.assertIsNone(table)


if __name__ == "__main__":
    unittest.main()
