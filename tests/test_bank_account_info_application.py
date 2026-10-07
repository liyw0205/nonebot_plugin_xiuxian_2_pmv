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
            connection.execute(
                "CREATE TABLE bank_account_operations("
                "operation_id TEXT PRIMARY KEY, user_id TEXT, payload TEXT, deposited INTEGER, "
                "interest INTEGER, wallet_after INTEGER, saved_after INTEGER, created_at TEXT)"
            )

    def tearDown(self) -> None:
        self.temp.cleanup()

    def test_reads_account_without_writing(self) -> None:
        with sqlite3.connect(self.database) as connection:
            tables_before = {
                row[0] for row in connection.execute("SELECT name FROM sqlite_master WHERE type='table'")
            }
        result = BankAccountInfoApplication(self.database).get_info(user_id="u1")
        self.assertEqual(result["status"], "ok")
        self.assertEqual(result["saved_stone"], 20)
        with sqlite3.connect(self.database) as connection:
            tables = {row[0] for row in connection.execute("SELECT name FROM sqlite_master WHERE type='table'")}
        self.assertEqual(tables, tables_before)

    def test_missing_migration_fails_without_creating_account_schema(self) -> None:
        database = Path(self.temp.name) / "unmigrated.db"
        with sqlite3.connect(database) as connection:
            connection.execute("CREATE TABLE user_xiuxian(user_id TEXT PRIMARY KEY, stone INTEGER)")
            connection.execute("INSERT INTO user_xiuxian VALUES ('u1', 80)")

        with self.assertRaisesRegex(RuntimeError, r"bank\.002 schema_missing: bank_accounts"):
            BankAccountInfoApplication(database).get_info(user_id="u1")

        with sqlite3.connect(database) as connection:
            tables = {row[0] for row in connection.execute("SELECT name FROM sqlite_master WHERE type='table'")}
        self.assertEqual(tables, {"user_xiuxian"})

    def test_missing_account_is_explicit(self) -> None:
        with sqlite3.connect(self.database) as connection:
            connection.execute("INSERT INTO user_xiuxian VALUES ('u2', 10)")
        result = BankAccountInfoApplication(self.database).get_info(user_id="u2")
        self.assertEqual(result, {"status": "account_missing", "user_id": "u2",
                                  "wallet_stone": 10, "saved_stone": 0,
                                  "bank_level": "1", "updated_at": ""})
        with sqlite3.connect(self.database) as connection:
            self.assertIsNone(connection.execute("SELECT user_id FROM bank_accounts WHERE user_id='u2'").fetchone())

    def test_missing_database_query_does_not_create_database(self) -> None:
        database = Path(self.temp.name) / "missing.db"
        result = BankAccountInfoApplication(database).get_info(user_id="u1")
        self.assertEqual(result, {"status": "schema_missing", "user_id": "u1"})
        self.assertFalse(database.exists())

    def test_legacy_player_account_is_not_read_during_account_query(self) -> None:
        with sqlite3.connect(self.database) as connection:
            connection.execute("INSERT INTO user_xiuxian VALUES ('u3', 80)")
        legacy = Path(self.temp.name) / "legacy-player.db"
        with sqlite3.connect(legacy) as connection:
            connection.execute(
                "CREATE TABLE bankinfo(user_id TEXT PRIMARY KEY,savestone INTEGER,savetime TEXT,banklevel TEXT)"
            )
            connection.execute("INSERT INTO bankinfo VALUES ('u3', 40, '2026-09-22 10:00:00', '2')")
        result = BankAccountInfoApplication(self.database, player_database=legacy).get_info(user_id="u3")
        self.assertEqual(result["status"], "account_missing")


if __name__ == "__main__":
    unittest.main()
