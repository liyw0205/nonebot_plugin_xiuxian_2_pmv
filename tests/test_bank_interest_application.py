from __future__ import annotations

import sqlite3
import tempfile
import unittest
from pathlib import Path

from nonebot_plugin_xiuxian_2.features.bank.account_application import BankDepositApplication
from nonebot_plugin_xiuxian_2.features.bank.account_interest_application import BankInterestApplication
from nonebot_plugin_xiuxian_2.features.bank.migrations import apply_bank_accounts
from nonebot_plugin_xiuxian_2.infrastructure.database import DatabaseUnitOfWork


class BankInterestApplicationTests(unittest.TestCase):
    def test_interest_credits_and_replays(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            database = Path(directory) / "game.db"
            with sqlite3.connect(database) as connection:
                connection.execute("CREATE TABLE user_xiuxian(user_id TEXT PRIMARY KEY, stone INTEGER)")
                connection.execute("INSERT INTO user_xiuxian VALUES ('u1', 100)")
            with DatabaseUnitOfWork(database) as uow:
                apply_bank_accounts(uow)
            BankDepositApplication(database).deposit(operation_id="d1", user_id="u1", amount=50, interest=0, limit=1000, bank_level="1", settled_at="t")
            application = BankInterestApplication(database)
            first = application.settle_interest(operation_id="i1", user_id="u1", interest=7, bank_level="1", settled_at="t2")
            duplicate = application.settle_interest(operation_id="i1", user_id="u1", interest=7, bank_level="1", settled_at="t2")
            self.assertEqual(first["status"], "applied")
            self.assertEqual(duplicate["status"], "duplicate")
            with sqlite3.connect(database) as connection:
                self.assertEqual(connection.execute("SELECT stone FROM user_xiuxian").fetchone()[0], 57)
                self.assertEqual(connection.execute("SELECT interest FROM bank_account_operations WHERE operation_id='i1'").fetchone()[0], 7)

    def test_missing_account_is_rejected(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            database = Path(directory) / "game.db"
            with sqlite3.connect(database) as connection:
                connection.execute("CREATE TABLE user_xiuxian(user_id TEXT PRIMARY KEY, stone INTEGER)")
                connection.execute("INSERT INTO user_xiuxian VALUES ('u1', 100)")
            with DatabaseUnitOfWork(database) as uow:
                apply_bank_accounts(uow)
            result = BankInterestApplication(database).settle_interest(operation_id="i1", user_id="u1", interest=7, bank_level="1", settled_at="t")
            self.assertEqual(result["status"], "user_missing")

    def test_first_interest_persists_default_account_and_settlement_atomically(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            database = Path(directory) / "game.db"
            with sqlite3.connect(database) as connection:
                connection.execute("CREATE TABLE user_xiuxian(user_id TEXT PRIMARY KEY, stone INTEGER)")
                connection.execute("INSERT INTO user_xiuxian VALUES ('u1', 100)")
            with DatabaseUnitOfWork(database) as uow:
                apply_bank_accounts(uow)

            application = BankInterestApplication(database)
            first = application.settle_interest(
                operation_id="first-interest",
                user_id="u1",
                interest=7,
                bank_level="1",
                settled_at="settled",
                initial_account={"saved_stone": 0, "bank_level": "1", "updated_at": "account-start"},
            )
            duplicate = application.settle_interest(
                operation_id="first-interest",
                user_id="u1",
                interest=7,
                bank_level="1",
                settled_at="settled",
            )
            self.assertEqual((first["status"], duplicate["status"]), ("applied", "duplicate"))
            self.assertEqual(first["saved_stone"], 0)
            with sqlite3.connect(database) as connection:
                self.assertEqual(connection.execute("SELECT stone FROM user_xiuxian WHERE user_id='u1'").fetchone()[0], 107)
                self.assertEqual(
                    connection.execute("SELECT saved_stone,bank_level,updated_at FROM bank_accounts WHERE user_id='u1'").fetchone(),
                    (0, "1", "settled"),
                )


if __name__ == "__main__":
    unittest.main()
