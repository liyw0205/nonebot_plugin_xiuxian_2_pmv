from __future__ import annotations

import sqlite3
import tempfile
import unittest
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import patch

from nonebot_plugin_xiuxian_2.features.bank.migrations import (
    apply_bank_accounts,
    apply_bank_legacy_accounts,
)
from nonebot_plugin_xiuxian_2.infrastructure.database import DatabaseUnitOfWork


class BankAccountsMigrationTests(unittest.TestCase):
    def test_new_bank_tables_are_idempotent(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            database = Path(directory) / "game.db"
            with DatabaseUnitOfWork(database) as uow:
                apply_bank_accounts(uow)
            with DatabaseUnitOfWork(database) as uow:
                apply_bank_accounts(uow)
            connection = sqlite3.connect(database)
            names = {row[0] for row in connection.execute("select name from sqlite_master where type='table'")}
            self.assertTrue({"bank_accounts", "bank_account_operations"}.issubset(names))
            connection.close()

    def setUpMigration(self, directory: str):
        root = Path(directory)
        game = root / "game.db"
        player = root / "player.db"
        with DatabaseUnitOfWork(game) as uow:
            apply_bank_accounts(uow)
        return game, player

    def test_legacy_backfill_reads_in_bounded_batches_and_records_audit(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            game, player = self.setUpMigration(directory)
            with sqlite3.connect(player) as connection:
                connection.execute(
                    "CREATE TABLE bankinfo(user_id TEXT PRIMARY KEY,savestone INTEGER,savetime TEXT,banklevel TEXT)"
                )
                connection.executemany(
                    "INSERT INTO bankinfo VALUES(?,?,?,?)",
                    [(f"u{i:03}", i, "legacy-time", "1") for i in range(405)],
                )
            before = player.stat().st_size

            with DatabaseUnitOfWork(game) as uow:
                apply_bank_legacy_accounts(uow)

            with DatabaseUnitOfWork(game, read_only=True) as uow:
                count = uow.query_one("SELECT COUNT(*) AS count FROM bank_accounts")["count"]
                audit = uow.query_one(
                    "SELECT source_rows,imported_rows,retained_rows "
                    "FROM bank_account_legacy_migration_audit WHERE source_table='player.bankinfo'"
                )
            self.assertEqual(count, 405)
            self.assertEqual(tuple(audit.values()), (405, 405, 0))
            self.assertEqual(player.stat().st_size, before)

    def test_newer_game_account_with_receipt_wins_over_legacy_snapshot(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            game, player = self.setUpMigration(directory)
            with DatabaseUnitOfWork(player) as uow:
                uow.execute(
                    "CREATE TABLE bankinfo(user_id TEXT PRIMARY KEY,savestone INTEGER,savetime TEXT,banklevel TEXT)"
                )
                uow.execute("INSERT INTO bankinfo VALUES('u',10,'old','1')")
            with DatabaseUnitOfWork(game) as uow:
                uow.execute("INSERT INTO bank_accounts VALUES('u',90,'2','new')")
                uow.execute(
                    "INSERT INTO bank_account_operations VALUES('op','u','[]',0,0,0,90,'now')"
                )

            with DatabaseUnitOfWork(game) as uow:
                apply_bank_legacy_accounts(uow)

            with DatabaseUnitOfWork(game, read_only=True) as uow:
                account = uow.query_one(
                    "SELECT saved_stone,bank_level,updated_at FROM bank_accounts WHERE user_id='u'"
                )
            self.assertEqual(tuple(account.values()), (90, "2", "new"))

    def test_conflicting_account_without_receipt_rolls_back_migration(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            game, player = self.setUpMigration(directory)
            with DatabaseUnitOfWork(player) as uow:
                uow.execute(
                    "CREATE TABLE bankinfo(user_id TEXT PRIMARY KEY,savestone INTEGER,savetime TEXT,banklevel TEXT)"
                )
                uow.execute("INSERT INTO bankinfo VALUES('u',10,'old','1')")
            with DatabaseUnitOfWork(game) as uow:
                uow.execute("INSERT INTO bank_accounts VALUES('u',90,'2','new')")

            with self.assertRaisesRegex(RuntimeError, "bank account migration conflict: u"):
                with DatabaseUnitOfWork(game) as uow:
                    apply_bank_legacy_accounts(uow)

            with DatabaseUnitOfWork(game, read_only=True) as uow:
                self.assertIsNone(
                    uow.query_one(
                        "SELECT 1 FROM sqlite_master WHERE type='table' "
                        "AND name='bank_account_legacy_migration_audit'"
                    )
                )
                account = uow.query_one("SELECT saved_stone FROM bank_accounts WHERE user_id='u'")
            self.assertEqual(account["saved_stone"], 90)

    def test_incomplete_legacy_schema_fails_without_audit(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            game, player = self.setUpMigration(directory)
            with sqlite3.connect(player) as connection:
                connection.execute("CREATE TABLE bankinfo(user_id TEXT PRIMARY KEY,savestone INTEGER)")

            with self.assertRaisesRegex(RuntimeError, "legacy bankinfo schema incomplete"):
                with DatabaseUnitOfWork(game) as uow:
                    apply_bank_legacy_accounts(uow)

    def test_low_disk_space_blocks_backfill_before_inserting_rows(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            game, player = self.setUpMigration(directory)
            with sqlite3.connect(player) as connection:
                connection.execute(
                    "CREATE TABLE bankinfo(user_id TEXT PRIMARY KEY,savestone INTEGER,savetime TEXT,banklevel TEXT)"
                )
                connection.execute("INSERT INTO bankinfo VALUES('u',10,'old','1')")

            with patch(
                "nonebot_plugin_xiuxian_2.features.bank.migrations.shutil.disk_usage",
                return_value=SimpleNamespace(free=1),
            ):
                with self.assertRaisesRegex(RuntimeError, "bank account migration needs about"):
                    with DatabaseUnitOfWork(game) as uow:
                        apply_bank_legacy_accounts(uow)

            with DatabaseUnitOfWork(game, read_only=True) as uow:
                self.assertEqual(uow.query_one("SELECT COUNT(*) AS count FROM bank_accounts")["count"], 0)

    def test_legacy_backfill_is_registered_for_game_db_only(self) -> None:
        from nonebot_plugin_xiuxian_2.plugin import build_migrations, migrations_for_database

        migrations = build_migrations()
        self.assertIn(
            "bank.003",
            {migration.version for migration in migrations_for_database(migrations, "game_db")},
        )
        self.assertNotIn(
            "bank.003",
            {migration.version for migration in migrations_for_database(migrations, "player_db")},
        )


if __name__ == "__main__":
    unittest.main()
