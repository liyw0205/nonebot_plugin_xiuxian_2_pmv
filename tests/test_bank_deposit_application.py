from __future__ import annotations

import sqlite3
import tempfile
import unittest
from pathlib import Path

from nonebot_plugin_xiuxian_2.features.bank.account_application import BankDepositApplication
from nonebot_plugin_xiuxian_2.features.bank.migrations import apply_bank_accounts
from nonebot_plugin_xiuxian_2.infrastructure.database import DatabaseUnitOfWork


class BankDepositApplicationTests(unittest.TestCase):
    def setUp(self) -> None:
        self.temp = tempfile.TemporaryDirectory()
        self.database = Path(self.temp.name) / "game.db"
        with sqlite3.connect(self.database) as connection:
            connection.execute("CREATE TABLE user_xiuxian(user_id TEXT PRIMARY KEY, stone INTEGER)")
            connection.execute("INSERT INTO user_xiuxian VALUES ('u1', 1000)")
        with DatabaseUnitOfWork(self.database) as uow:
            apply_bank_accounts(uow)
        self.application = BankDepositApplication(self.database)

    def tearDown(self) -> None:
        self.temp.cleanup()

    def test_first_use_deposit_and_replay(self) -> None:
        first = self.application.deposit(operation_id="bank-1", user_id="u1", amount=300, interest=10, limit=1000, bank_level="1", settled_at="2026-09-13T00:00:00Z")
        duplicate = self.application.deposit(operation_id="bank-1", user_id="u1", amount=300, interest=10, limit=1000, bank_level="1", settled_at="2026-09-13T00:00:00Z")
        self.assertEqual(first["status"], "applied")
        self.assertEqual(duplicate["status"], "duplicate")
        with sqlite3.connect(self.database) as connection:
            self.assertEqual(connection.execute("SELECT stone FROM user_xiuxian WHERE user_id='u1'").fetchone()[0], 710)
            self.assertEqual(connection.execute("SELECT saved_stone FROM bank_accounts WHERE user_id='u1'").fetchone()[0], 300)

    def test_conflict_does_not_change_assets(self) -> None:
        self.application.deposit(operation_id="bank-1", user_id="u1", amount=300, interest=0, limit=1000, bank_level="1", settled_at="t")
        conflict = self.application.deposit(operation_id="bank-1", user_id="u1", amount=301, interest=0, limit=1000, bank_level="1", settled_at="t")
        self.assertEqual(conflict["status"], "operation_conflict")

    def test_insufficient_wallet_rolls_back(self) -> None:
        result = self.application.deposit(operation_id="bank-2", user_id="u1", amount=2000, interest=0, limit=5000, bank_level="1", settled_at="t")
        self.assertEqual(result["status"], "stone_insufficient")
        with sqlite3.connect(self.database) as connection:
            self.assertEqual(connection.execute("SELECT stone FROM user_xiuxian WHERE user_id='u1'").fetchone()[0], 1000)
            self.assertEqual(connection.execute("SELECT count(*) FROM bank_accounts").fetchone()[0], 0)


if __name__ == "__main__":
    unittest.main()
