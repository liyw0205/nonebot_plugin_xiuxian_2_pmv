from __future__ import annotations

import sqlite3
import tempfile
import unittest
from pathlib import Path

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
            connection.execute("CREATE TABLE bankinfo(user_id TEXT PRIMARY KEY, savestone TEXT, banklevel TEXT)")
            connection.execute("INSERT INTO bankinfo VALUES('u', 'invalid', '3')")
        with sqlite3.connect(self.player_database) as connection:
            before = tuple(row[1] for row in connection.execute('PRAGMA table_info("bankinfo")'))

        result = self.application.get_legacy_info(user_id="u", default_saved_at="now")

        self.assertEqual(result, {"savestone": 0, "savetime": "now", "banklevel": "3"})
        with sqlite3.connect(self.player_database) as connection:
            after = tuple(row[1] for row in connection.execute('PRAGMA table_info("bankinfo")'))
        self.assertEqual(after, before)

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
