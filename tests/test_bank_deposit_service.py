from __future__ import annotations

import tempfile
import unittest
from pathlib import Path

import nonebot

nonebot.init()

from nonebot_plugin_xiuxian_2.xiuxian.xiuxian_bank.deposit_service import BankDepositService
from tests.test_db_backend import db_backend


class BankDepositServiceTests(unittest.TestCase):
    def setUp(self) -> None:
        self.temp_dir = tempfile.TemporaryDirectory()
        root = Path(self.temp_dir.name)
        self.game_database = root / "game.sqlite3"
        self.player_database = root / "player.sqlite3"
        with db_backend.transaction(self.game_database) as conn:
            conn.execute("CREATE TABLE user_xiuxian (user_id TEXT PRIMARY KEY, stone INTEGER NOT NULL)")
            conn.execute("INSERT INTO user_xiuxian VALUES (%s, %s)", ("user", 1000))
        with db_backend.transaction(self.player_database) as conn:
            conn.execute(
                "CREATE TABLE bankinfo (user_id TEXT PRIMARY KEY, savestone INTEGER, savetime TEXT, banklevel TEXT)"
            )
            conn.execute("INSERT INTO bankinfo VALUES (%s, %s, %s, %s)", ("user", 200, "old", "1"))
        self.service = BankDepositService(self.game_database, self.player_database)

    def tearDown(self) -> None:
        self.temp_dir.cleanup()

    def state(self):
        with db_backend.connection(self.game_database) as conn:
            stone = int(conn.execute("SELECT stone FROM user_xiuxian WHERE user_id=%s", ("user",)).fetchone()[0])
        with db_backend.connection(self.player_database) as conn:
            row = conn.execute("SELECT savestone, savetime, banklevel FROM bankinfo WHERE user_id=%s", ("user",)).fetchone()
        return stone, (int(row[0]), str(row[1]), str(row[2]))

    def deposit(self, operation_id="deposit", amount=300, expected_saved=200, interest=20, settled_at="new"):
        return self.service.deposit(
            operation_id, "user", amount, expected_saved, "old", "1", interest, settled_at, 1000
        )

    def test_success_charges_principal_grants_interest_and_updates_account(self) -> None:
        result = self.deposit()
        self.assertEqual((result.status, result.wallet_stone, result.saved_stone), ("applied", 720, 500))
        self.assertEqual(self.state(), (720, (500, "new", "1")))

    def test_insufficient_wallet_changes_nothing(self) -> None:
        result = self.deposit(amount=1100)
        self.assertEqual(result.status, "stone_insufficient")
        self.assertEqual(self.state(), (1000, (200, "old", "1")))

    def test_limit_exceeded_changes_nothing(self) -> None:
        result = self.deposit(amount=900)
        self.assertEqual(result.status, "limit_exceeded")
        self.assertEqual(self.state(), (1000, (200, "old", "1")))

    def test_stale_account_state_changes_nothing(self) -> None:
        result = self.deposit(expected_saved=100)
        self.assertEqual(result.status, "state_changed")
        self.assertEqual(self.state(), (1000, (200, "old", "1")))

    def test_duplicate_reuses_result_and_conflict_is_rejected(self) -> None:
        first = self.deposit("repeat")
        duplicate = self.deposit("repeat")
        conflict = self.deposit("repeat", amount=301)
        self.assertEqual((first.status, duplicate.status, conflict.status), ("applied", "duplicate", "state_changed"))
        self.assertEqual(duplicate, first.__class__("duplicate", 300, 20, 720, 500, "new"))
        self.assertEqual(self.state(), (720, (500, "new", "1")))

    def test_operation_failure_rolls_back_wallet_and_account(self) -> None:
        with db_backend.transaction(self.game_database) as conn:
            conn.execute(
                "CREATE TABLE bank_deposit_operations (operation_id TEXT PRIMARY KEY, payload TEXT NOT NULL, "
                "deposited INTEGER NOT NULL, interest INTEGER NOT NULL, wallet_stone INTEGER NOT NULL, "
                "saved_stone INTEGER NOT NULL, saved_at TEXT NOT NULL, created_at TIMESTAMP)"
            )
            conn.execute(
                "CREATE TRIGGER fail_deposit BEFORE INSERT ON bank_deposit_operations "
                "BEGIN SELECT RAISE(ABORT, 'failed'); END"
            )
        with self.assertRaises(db_backend.IntegrityError):
            self.deposit("rollback")
        self.assertEqual(self.state(), (1000, (200, "old", "1")))


if __name__ == "__main__":
    unittest.main()
