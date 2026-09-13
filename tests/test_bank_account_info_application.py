from __future__ import annotations

import sqlite3
import tempfile
import unittest
from pathlib import Path

from nonebot_plugin_xiuxian_2.features.bank.account_info_application import BankAccountInfoApplication


class BankAccountInfoApplicationTests(unittest.TestCase):
    def setUp(self) -> None:
        self.temp = tempfile.TemporaryDirectory()
        self.database = Path(self.temp.name) / "game.db"
        with sqlite3.connect(self.database) as connection:
            connection.execute("CREATE TABLE user_xiuxian(user_id TEXT PRIMARY KEY, stone INTEGER)")
            connection.execute("INSERT INTO user_xiuxian VALUES ('u1', 80)")
            connection.execute("CREATE TABLE bank_accounts(user_id TEXT PRIMARY KEY, saved_stone INTEGER, bank_level TEXT, updated_at TEXT)")
            connection.execute("INSERT INTO bank_accounts VALUES ('u1', 20, '1', 't')")

    def tearDown(self) -> None:
        self.temp.cleanup()

    def test_reads_account_without_writing(self) -> None:
        result = BankAccountInfoApplication(self.database).get_info(user_id="u1")
        self.assertEqual(result["status"], "ok")
        self.assertEqual(result["saved_stone"], 20)
        with sqlite3.connect(self.database) as connection:
            tables = {row[0] for row in connection.execute("SELECT name FROM sqlite_master WHERE type='table'")}
        self.assertNotIn("bank_account_operations", tables)

    def test_missing_account_is_explicit(self) -> None:
        with sqlite3.connect(self.database) as connection:
            connection.execute("INSERT INTO user_xiuxian VALUES ('u2', 10)")
        self.assertEqual(BankAccountInfoApplication(self.database).get_info(user_id="u2")["status"], "account_missing")


if __name__ == "__main__":
    unittest.main()
