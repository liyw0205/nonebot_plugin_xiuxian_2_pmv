from __future__ import annotations

import sqlite3
import tempfile
import unittest
from pathlib import Path

from nonebot_plugin_xiuxian_2.features.bank.account_application import BankDepositApplication
from nonebot_plugin_xiuxian_2.features.bank.account_upgrade_application import BankUpgradeApplication
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
            BankDepositApplication(database).deposit(operation_id="d1", user_id="u1", amount=100, interest=0, limit=1000, bank_level="1", settled_at="t")
            app = BankUpgradeApplication(database)
            first = app.upgrade(operation_id="u1", user_id="u1", expected_level="1", next_level="2", cost=200000, settled_at="t2")
            duplicate = app.upgrade(operation_id="u1", user_id="u1", expected_level="1", next_level="2", cost=200000, settled_at="t2")
            self.assertEqual(first["status"], "applied")
            self.assertEqual(duplicate["status"], "duplicate")
            with sqlite3.connect(database) as connection:
                self.assertEqual(connection.execute("SELECT stone FROM user_xiuxian").fetchone()[0], 299900)
                self.assertEqual(connection.execute("SELECT bank_level FROM bank_accounts").fetchone()[0], "2")


if __name__ == "__main__":
    unittest.main()
