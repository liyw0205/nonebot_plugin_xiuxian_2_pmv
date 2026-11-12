from __future__ import annotations

import sqlite3
import tempfile
import unittest
from pathlib import Path

from nonebot_plugin_xiuxian_2.core.errors import DomainError
from nonebot_plugin_xiuxian_2.features.bank.account_info_application import BankAccountInfoApplication


class BankLegacyAccountReadTests(unittest.TestCase):
    def setUp(self) -> None:
        self.temp_dir = tempfile.TemporaryDirectory()
        root = Path(self.temp_dir.name)
        self.game_database = root / "game.sqlite3"
        self.player_database = root / "player.sqlite3"
        self.application = BankAccountInfoApplication(
            self.game_database,
            player_database=self.player_database,
        )

    def tearDown(self) -> None:
        self.temp_dir.cleanup()

    def test_missing_legacy_database_returns_defaults_without_creating_it(self) -> None:
        result = self.application.get_legacy_info(user_id="u", default_saved_at="now")

        self.assertEqual(result, {"savestone": 0, "savetime": "now", "banklevel": "1"})
        self.assertFalse(self.player_database.exists())
        self.assertEqual(self.application.legacy_record_status(user_id="u"), "missing")

    def test_missing_table_returns_defaults_without_creating_schema(self) -> None:
        with sqlite3.connect(self.player_database) as connection:
            connection.execute("CREATE TABLE unrelated(id INTEGER PRIMARY KEY)")

        result = self.application.get_legacy_info(user_id="u", default_saved_at="now")

        self.assertEqual(result, {"savestone": 0, "savetime": "now", "banklevel": "1"})
        with sqlite3.connect(self.player_database) as connection:
            tables = {row[0] for row in connection.execute("SELECT name FROM sqlite_master WHERE type='table'")}
        self.assertEqual(tables, {"unrelated"})

    def test_partial_legacy_row_uses_existing_field_defaults_without_schema_changes(self) -> None:
        with sqlite3.connect(self.player_database) as connection:
            connection.execute("CREATE TABLE bankinfo(user_id TEXT PRIMARY KEY, banklevel TEXT)")
            connection.execute("INSERT INTO bankinfo VALUES('u', '3')")
        with sqlite3.connect(self.player_database) as connection:
            before = tuple(row[1] for row in connection.execute('PRAGMA table_info("bankinfo")'))

        result = self.application.get_legacy_info(user_id="u", default_saved_at="now")

        self.assertEqual(result, {"savestone": 0, "savetime": "now", "banklevel": "3"})
        with sqlite3.connect(self.player_database) as connection:
            after = tuple(row[1] for row in connection.execute('PRAGMA table_info("bankinfo")'))
        self.assertEqual(after, before)
        self.assertEqual(self.application.legacy_record_status(user_id="u"), "present")

    def test_invalid_existing_balance_is_not_defaulted_to_zero(self) -> None:
        with sqlite3.connect(self.player_database) as connection:
            connection.execute("CREATE TABLE bankinfo(user_id TEXT PRIMARY KEY, savestone, banklevel TEXT)")
            connection.execute("INSERT INTO bankinfo VALUES('u', 0, '3')")
        for value, reason in (("invalid", "saved_stone_invalid_numeric_value"),
                              ("1e-2", "saved_stone_invalid_numeric_value"),
                              (float(2**63), "saved_stone_precision_lost_real")):
            with self.subTest(value=value):
                with sqlite3.connect(self.player_database) as connection:
                    connection.execute("UPDATE bankinfo SET savestone=?", (value,))
                    before = tuple(connection.iterdump())
                with self.assertRaises(DomainError) as caught:
                    self.application.get_legacy_info(user_id="u", default_saved_at="now")
                self.assertEqual((reason, "u"), (caught.exception.details["reason"], caught.exception.details["subject"]))
                with sqlite3.connect(self.player_database) as connection:
                    self.assertEqual(before, tuple(connection.iterdump()))
                self.assertFalse(self.game_database.exists())

    def test_legacy_table_without_user_id_is_invalid_for_write_fallbacks(self) -> None:
        with sqlite3.connect(self.player_database) as connection:
            connection.execute("CREATE TABLE bankinfo(savestone INTEGER)")

        self.assertEqual(self.application.legacy_record_status(user_id="u"), "invalid")

    def test_missing_legacy_user_row_is_distinct_from_an_existing_partial_row(self) -> None:
        with sqlite3.connect(self.player_database) as connection:
            connection.execute("CREATE TABLE bankinfo(user_id TEXT PRIMARY KEY, savestone INTEGER)")

        self.assertEqual(self.application.legacy_record_status(user_id="u"), "missing")

    def test_full_legacy_row_is_returned_with_normalized_values(self) -> None:
        with sqlite3.connect(self.player_database) as connection:
            connection.execute(
                "CREATE TABLE bankinfo(user_id TEXT PRIMARY KEY, savestone TEXT, savetime TEXT, banklevel TEXT)"
            )
            connection.execute("INSERT INTO bankinfo VALUES('u', '80', 'then', '2')")

        self.assertEqual(
            self.application.get_legacy_info(user_id="u", default_saved_at="now"),
            {"savestone": 80, "savetime": "then", "banklevel": "2"},
        )


class LegacyBankAccountStorageCompatibilityTests(unittest.TestCase):
    def test_savef_keeps_legacy_field_updates_behind_compatibility_module(self) -> None:
        import nonebot
        from unittest.mock import patch

        try:
            nonebot.get_driver()
        except ValueError:
            nonebot.init()

        from nonebot_plugin_xiuxian_2.compatibility import legacy_bank_account_storage
        from nonebot_plugin_xiuxian_2.xiuxian.xiuxian_bank import savef

        class Manager:
            def __init__(self) -> None:
                self.updates = []

            def update_or_write_data(self, *args, **kwargs) -> None:
                self.updates.append((args, kwargs))

        manager = Manager()
        with patch.object(legacy_bank_account_storage, "_resolve_player_data_manager", return_value=manager):
            self.assertTrue(savef("u", {"savestone": 8, "savetime": "then", "banklevel": "2"}))

        self.assertEqual(
            manager.updates,
            [
                (("u", "bankinfo", "savestone", 8), {"data_type": "INTEGER"}),
                (("u", "bankinfo", "savetime", "then"), {"data_type": "TEXT"}),
                (("u", "bankinfo", "banklevel", "2"), {"data_type": "TEXT"}),
            ],
        )


if __name__ == "__main__":
    unittest.main()
