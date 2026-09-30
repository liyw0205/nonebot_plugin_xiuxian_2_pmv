from __future__ import annotations

import json
import sqlite3
import tempfile
import unittest
from pathlib import Path

import nonebot

nonebot.init()

from nonebot_plugin_xiuxian_2.features.bank.migrations import apply_bank_accounts
from nonebot_plugin_xiuxian_2.infrastructure.database import DatabaseUnitOfWork
from nonebot_plugin_xiuxian_2.xiuxian.xiuxian_buff import _migrate_bank_data_sync


class BankMigrationIoTests(unittest.TestCase):
    def test_migration_reads_valid_files_and_counts_failures(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            players = root / "players"
            players.mkdir()
            valid = players / "1001"
            invalid = players / "1002"
            empty = players / "1003"
            valid.mkdir()
            invalid.mkdir()
            empty.mkdir()
            (valid / "bankinfo.json").write_text(
                json.dumps(
                    {
                        "savestone": "120",
                        "savetime": "2026-07-12 12:00:00",
                        "banklevel": 3,
                    }
                ),
                encoding="utf-8",
            )
            (invalid / "bankinfo.json").write_text("{broken", encoding="utf-8")
            (empty / "bankinfo.json").write_text("", encoding="utf-8")

            game = root / "game.db"
            player = root / "player.db"
            with DatabaseUnitOfWork(game) as uow:
                uow.execute("CREATE TABLE user_xiuxian(user_id TEXT PRIMARY KEY,stone INTEGER)")
                uow.execute("INSERT INTO user_xiuxian VALUES('1001',1000)")
                apply_bank_accounts(uow)
            with sqlite3.connect(player) as connection:
                connection.execute(
                    "CREATE TABLE bankinfo(user_id TEXT PRIMARY KEY,savestone INTEGER,savetime TEXT,banklevel TEXT)"
                )
                connection.execute("INSERT INTO bankinfo VALUES('1001',5,'old','1')")

            user_num, sync_num, fail_num = _migrate_bank_data_sync(players, game)
            self.assertEqual((user_num, sync_num, fail_num), (3, 1, 1))
            with DatabaseUnitOfWork(game, read_only=True) as uow:
                account = uow.query_one(
                    "SELECT saved_stone,bank_level,updated_at FROM bank_accounts WHERE user_id='1001'"
                )
            self.assertEqual(tuple(account.values()), (120, "3", "2026-07-12 12:00:00"))
            with sqlite3.connect(player) as connection:
                self.assertEqual(
                    connection.execute("SELECT savestone FROM bankinfo WHERE user_id='1001'").fetchone()[0],
                    5,
                )

    def test_sync_does_not_overwrite_existing_game_account_or_create_for_missing_user(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            players = root / "players"
            players.mkdir()
            existing = players / "existing"
            missing = players / "missing"
            existing.mkdir()
            missing.mkdir()
            payload = {"savestone": 120, "savetime": "legacy-time", "banklevel": "3"}
            (existing / "bankinfo.json").write_text(json.dumps(payload), encoding="utf-8")
            (missing / "bankinfo.json").write_text(json.dumps(payload), encoding="utf-8")
            game = root / "game.db"
            with DatabaseUnitOfWork(game) as uow:
                uow.execute("CREATE TABLE user_xiuxian(user_id TEXT PRIMARY KEY,stone INTEGER)")
                uow.execute("INSERT INTO user_xiuxian VALUES('existing',1000)")
                apply_bank_accounts(uow)
                uow.execute("INSERT INTO bank_accounts VALUES('existing',700,'2','game-time')")

            self.assertEqual(_migrate_bank_data_sync(players, game), (2, 1, 1))
            with DatabaseUnitOfWork(game, read_only=True) as uow:
                account = uow.query_one(
                    "SELECT saved_stone,bank_level,updated_at FROM bank_accounts WHERE user_id='existing'"
                )
                count = uow.query_one("SELECT COUNT(*) AS count FROM bank_accounts")["count"]
            self.assertEqual(tuple(account.values()), (700, "2", "game-time"))
            self.assertEqual(count, 1)

    def test_missing_players_directory_returns_zero_counts(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            missing = Path(directory) / "missing"
            self.assertEqual(_migrate_bank_data_sync(missing, Path(directory) / "game.db"), (0, 0, 0))


if __name__ == "__main__":
    unittest.main()
