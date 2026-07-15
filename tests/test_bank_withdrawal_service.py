from __future__ import annotations

import tempfile
import unittest
from pathlib import Path

import nonebot

nonebot.init()

from nonebot_plugin_xiuxian_2.xiuxian.xiuxian_bank.transaction_service import BankWithdrawalService
from tests.test_db_backend import db_backend


class BankWithdrawalServiceTests(unittest.TestCase):
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
            conn.execute("INSERT INTO bankinfo VALUES (%s, %s, %s, %s)", ("user", 500, "old", "1"))
        self.service = BankWithdrawalService(self.game_database, self.player_database)

    def tearDown(self) -> None:
        self.temp_dir.cleanup()

    def state(self):
        with db_backend.connection(self.game_database) as conn:
            stone = int(conn.execute("SELECT stone FROM user_xiuxian WHERE user_id=%s", ("user",)).fetchone()[0])
        with db_backend.connection(self.player_database) as conn:
            row = conn.execute("SELECT savestone, savetime, banklevel FROM bankinfo WHERE user_id=%s", ("user",)).fetchone()
        return stone, (int(row[0]), str(row[1]), str(row[2]))

    def withdraw(self, operation_id="withdraw", amount=300, expected_saved=500, interest=20, settled_at="new"):
        return self.service.withdraw(
            operation_id, "user", amount, expected_saved, "old", "1", interest, settled_at
        )

    def test_success_credits_principal_and_interest_and_updates_account(self) -> None:
        result = self.withdraw()
        self.assertEqual((result.status, result.wallet_stone, result.saved_stone), ("applied", 1320, 200))
        self.assertEqual(self.state(), (1320, (200, "new", "1")))

    def test_insufficient_saved_stone_changes_nothing(self) -> None:
        result = self.withdraw(amount=600)
        self.assertEqual(result.status, "saved_stone_insufficient")
        self.assertEqual(self.state(), (1000, (500, "old", "1")))

    def test_stale_account_state_changes_nothing(self) -> None:
        result = self.withdraw(expected_saved=400)
        self.assertEqual(result.status, "state_changed")
        self.assertEqual(self.state(), (1000, (500, "old", "1")))

    def test_duplicate_reuses_result_and_conflict_is_rejected(self) -> None:
        first = self.withdraw("repeat")
        duplicate = self.withdraw("repeat", interest=99, settled_at="later")
        self.assertEqual((first.status, duplicate.status), ("applied", "duplicate"))
        self.assertEqual((duplicate.withdrawn, duplicate.interest, duplicate.wallet_stone), (300, 20, 1320))
        conflict = self.withdraw("repeat", amount=301)
        self.assertEqual(conflict.status, "state_changed")
        self.assertEqual(self.state(), (1320, (200, "new", "1")))
        prior = self.service.get_result("repeat")
        self.assertIsNotNone(prior)
        self.assertEqual(prior.withdrawn, 300)

    def test_operation_failure_rolls_back_wallet_and_account(self) -> None:
        with db_backend.transaction(self.game_database) as conn:
            conn.execute(
                "CREATE TABLE bank_withdrawal_operations (operation_id TEXT PRIMARY KEY, payload TEXT NOT NULL, "
                "withdrawn INTEGER NOT NULL, interest INTEGER NOT NULL, wallet_stone INTEGER NOT NULL, "
                "saved_stone INTEGER NOT NULL, saved_at TEXT NOT NULL, created_at TIMESTAMP)"
            )
            conn.execute(
                "CREATE TRIGGER fail_withdrawal BEFORE INSERT ON bank_withdrawal_operations "
                "BEGIN SELECT RAISE(ABORT, 'failed'); END"
            )
        with self.assertRaises(db_backend.IntegrityError):
            self.withdraw("rollback")
        self.assertEqual(self.state(), (1000, (500, "old", "1")))


if __name__ == "__main__":
    unittest.main()
