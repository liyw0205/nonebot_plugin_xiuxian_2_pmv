from __future__ import annotations

import sqlite3
import tempfile
import unittest
from pathlib import Path

from nonebot_plugin_xiuxian_2.features.bank.account_application import BankDepositApplication
from nonebot_plugin_xiuxian_2.features.bank.account_upgrade_application import BankUpgradeApplication
from nonebot_plugin_xiuxian_2.features.bank.migrations import apply_bank_accounts
from nonebot_plugin_xiuxian_2.infrastructure.database import DatabaseUnitOfWork
from nonebot_plugin_xiuxian_2.features.bank.upgrade_rules import decide_upgrade


class BankUpgradeTests(unittest.TestCase):
    def test_rule_rejects_insufficient_wallet(self) -> None:
        with self.assertRaisesRegex(ValueError, "stone_insufficient"):
            decide_upgrade(wallet=10, current_level="1", expected_level="1", next_level="2", cost=11)

    def test_application_upgrades_and_replays(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            database = Path(directory) / "game.db"
            with sqlite3.connect(database) as connection:
                connection.execute("CREATE TABLE user_xiuxian(user_id TEXT PRIMARY KEY, stone INTEGER)")
                connection.execute("INSERT INTO user_xiuxian VALUES ('u1', 500000)")
            with DatabaseUnitOfWork(database) as uow:
                apply_bank_accounts(uow)
            BankDepositApplication(database).deposit(operation_id="d1", user_id="u1", amount=100, interest=0, limit=1000, bank_level="1", settled_at="t")
            app = BankUpgradeApplication(database)
            first = app.upgrade(operation_id="u1", user_id="u1", expected_level="1", next_level="2", cost=200000, settled_at="t2")
            duplicate = app.upgrade(operation_id="u1", user_id="u1", expected_level="1", next_level="2", cost=200000, settled_at="t2")
            self.assertEqual(first["status"], "applied")
            self.assertEqual(duplicate["status"], "duplicate")
            self.assertEqual(duplicate["cost"], 200000)
            with sqlite3.connect(database) as connection:
                self.assertEqual(connection.execute("SELECT stone FROM user_xiuxian").fetchone()[0], 299900)
                self.assertEqual(connection.execute("SELECT bank_level FROM bank_accounts").fetchone()[0], "2")
                self.assertEqual(connection.execute("SELECT updated_at FROM bank_accounts").fetchone()[0], "t")

    def test_first_upgrade_persists_default_account_atomically(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            database = Path(directory) / "game.db"
            with sqlite3.connect(database) as connection:
                connection.execute("CREATE TABLE user_xiuxian(user_id TEXT PRIMARY KEY, stone INTEGER)")
                connection.execute("INSERT INTO user_xiuxian VALUES ('u1', 500000)")
            with DatabaseUnitOfWork(database) as uow:
                apply_bank_accounts(uow)

            result = BankUpgradeApplication(database).upgrade(
                operation_id="first-upgrade",
                user_id="u1",
                expected_level="1",
                next_level="2",
                cost=200000,
                settled_at="upgrade-time",
                initial_account={"saved_stone": 0, "bank_level": "1", "updated_at": "account-start"},
            )
            self.assertEqual(result["status"], "applied")
            with sqlite3.connect(database) as connection:
                self.assertEqual(connection.execute("SELECT stone FROM user_xiuxian WHERE user_id='u1'").fetchone()[0], 300000)
                self.assertEqual(
                    connection.execute("SELECT saved_stone,bank_level,updated_at FROM bank_accounts WHERE user_id='u1'").fetchone(),
                    (0, "2", "account-start"),
                )

    def test_failed_first_upgrade_does_not_persist_default_account(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            database = Path(directory) / "game.db"
            with sqlite3.connect(database) as connection:
                connection.execute("CREATE TABLE user_xiuxian(user_id TEXT PRIMARY KEY, stone INTEGER)")
                connection.execute("INSERT INTO user_xiuxian VALUES ('u1', 100)")
            with DatabaseUnitOfWork(database) as uow:
                apply_bank_accounts(uow)

            result = BankUpgradeApplication(database).upgrade(
                operation_id="insufficient-first-upgrade",
                user_id="u1",
                expected_level="1",
                next_level="2",
                cost=200000,
                settled_at="upgrade-time",
                initial_account={"saved_stone": 0, "bank_level": "1", "updated_at": "account-start"},
            )
            self.assertEqual(result["status"], "stone_insufficient")
            with sqlite3.connect(database) as connection:
                self.assertIsNone(connection.execute("SELECT 1 FROM bank_accounts WHERE user_id='u1'").fetchone())


if __name__ == "__main__":
    unittest.main()
