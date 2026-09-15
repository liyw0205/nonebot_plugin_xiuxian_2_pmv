from __future__ import annotations

import sqlite3
import tempfile
import unittest
from pathlib import Path

from nonebot_plugin_xiuxian_2.features.bank.account_application import BankDepositApplication
from nonebot_plugin_xiuxian_2.features.bank.account_withdrawal_application import BankWithdrawalApplication
from nonebot_plugin_xiuxian_2.features.bank.migrations import apply_bank_accounts
from nonebot_plugin_xiuxian_2.infrastructure.database import DatabaseUnitOfWork
from nonebot_plugin_xiuxian_2.features.bank.withdrawal_rules import decide_withdraw


class BankWithdrawalTests(unittest.TestCase):
    def test_rule_rejects_insufficient_saved(self) -> None:
        with self.assertRaisesRegex(ValueError, "saved_stone_insufficient"):
            decide_withdraw(wallet=0, saved=10, amount=11, interest=0)

    def test_application_withdraws_and_replays(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            database = Path(directory) / "game.db"
            with sqlite3.connect(database) as connection:
                connection.execute("CREATE TABLE user_xiuxian(user_id TEXT PRIMARY KEY, stone INTEGER)")
                connection.execute("INSERT INTO user_xiuxian VALUES ('u1', 700)")
            with DatabaseUnitOfWork(database) as uow:
                apply_bank_accounts(uow)
            deposit = BankDepositApplication(database)
            deposit.deposit(operation_id="d1", user_id="u1", amount=300, interest=0, limit=1000, bank_level="1", settled_at="t")
            withdrawal = BankWithdrawalApplication(database)
            first = withdrawal.withdraw(operation_id="w1", user_id="u1", amount=100, interest=5, bank_level="1", settled_at="t2")
            duplicate = withdrawal.withdraw(operation_id="w1", user_id="u1", amount=100, interest=5, bank_level="1", settled_at="t2")
            self.assertEqual(first["status"], "applied")
            self.assertEqual(duplicate["status"], "duplicate")
            with sqlite3.connect(database) as connection:
                self.assertEqual(connection.execute("SELECT stone FROM user_xiuxian").fetchone()[0], 505)
                self.assertEqual(connection.execute("SELECT saved_stone FROM bank_accounts").fetchone()[0], 200)


if __name__ == "__main__":
    unittest.main()
